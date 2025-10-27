/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.auron.plan

import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.hashing.byteswap32

import org.apache.spark.{OneToOneDependency, Partitioner, RangePartitioner, ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleWriteProcessor
import org.apache.spark.sql.auron.NativeConverters
import org.apache.spark.sql.auron.NativeHelper
import org.apache.spark.sql.auron.NativeRDD
import org.apache.spark.sql.auron.NativeSupports
import org.apache.spark.sql.auron.Shims
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, BoundReference, NullsFirst, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, RangePartitioning, RoundRobinPartitioning, SinglePartition}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution, UnsafeRowSerializer}
import org.apache.spark.sql.execution.auron.shuffle.AuronBlockStoreShuffleReaderBase
import org.apache.spark.sql.execution.auron.shuffle.AuronShuffleDependency
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.util.{CompletionIterator, MutablePair}

import org.apache.auron.jni.JniBridge
import org.apache.auron.metric.SparkMetricNode
import org.apache.auron.protobuf.{IpcReaderExecNode, PhysicalExprNode, PhysicalHashRepartition, PhysicalPlanNode, PhysicalRangeRepartition, PhysicalRepartition, PhysicalRoundRobinRepartition, PhysicalSingleRepartition, PhysicalSortExprNode, Schema, SortExecNode}

abstract class NativeShuffleExchangeBase(
    override val outputPartitioning: Partitioning,
    override val child: SparkPlan)
    extends ShuffleExchangeLike
    with NativeSupports {

  override val nodeName: String = "NativeShuffleExchange"

  val serializer: UnsafeRowSerializer =
    new UnsafeRowSerializer(child.output.size, longMetric("dataSize"))

  @transient
  lazy val inputRDD: RDD[InternalRow] = if (NativeHelper.isNative(child)) {
    NativeHelper.executeNative(child)
  } else {
    child.execute()
  }

  /**
   * A [[ShuffleDependency]] that will partition rows of its child based on the partitioning
   * scheme defined in `newPartitioning`. Those partitions of the returned ShuffleDependency will
   * be the input of shuffle.
   */
  @transient
  lazy val shuffleDependency: ShuffleDependency[Int, InternalRow, InternalRow] = {
    prepareNativeShuffleDependency(
      inputRDD,
      child.output,
      outputPartitioning,
      serializer,
      metrics)
  }

  def nativeSchema: Schema = Util.getNativeSchema(child.output)

  private def nativeHashExprs = outputPartitioning match {
    case HashPartitioning(expressions, _) =>
      expressions.map(expr => NativeConverters.convertExpr(expr)).toList
    case _ => null
  }

  private def nativeSortExecNode = outputPartitioning match {
    case RangePartitioning(expressions, _) =>
      val nativeSortExprs = expressions.map { sortOrder =>
        PhysicalExprNode
          .newBuilder()
          .setSort(
            PhysicalSortExprNode
              .newBuilder()
              .setExpr(NativeConverters.convertExpr(sortOrder.child))
              .setAsc(sortOrder.direction == Ascending)
              .setNullsFirst(sortOrder.nullOrdering == NullsFirst)
              .build())
          .build()
      }
      SortExecNode
        .newBuilder()
        .addAllExpr(nativeSortExprs.asJava)
        .build()
    case _ => null
  }

  // check whether native converting is supported
  nativeSchema
  nativeHashExprs
  nativeSortExecNode

  protected def doExecuteNonNative(): RDD[InternalRow]

  override def doExecuteNative(): NativeRDD = {
    val shuffleHandle = shuffleDependency.shuffleHandle
    val rdd = doExecuteNonNative()

    val nativeMetrics = SparkMetricNode(
      Map(),
      Nil,
      Some({
        case ("output_rows", v) =>
          val shuffleReadMetrics = TaskContext.get.taskMetrics().createTempShuffleReadMetrics()
          new SQLShuffleReadMetricsReporter(shuffleReadMetrics, metrics).incRecordsRead(v)
          TaskContext.get.taskMetrics().mergeShuffleReadMetrics()
        case ("elapsed_compute", v) => metrics("shuffle_read_total_time") += v
        case _ =>
      }))

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      rddPartitions = rdd.partitions,
      rddPartitioner = rdd.partitioner,
      rddDependencies = shuffleDependency :: Nil,
      Shims.get.getRDDShuffleReadFull(rdd),
      (partition, taskContext) => {
        val shuffleReadMetrics = taskContext.taskMetrics().createTempShuffleReadMetrics()
        val metricReporter = new SQLShuffleReadMetricsReporter(shuffleReadMetrics, metrics)
        val nativeSchema = this.nativeSchema

        // store fetch iterator in jni resource before native compute
        val jniResourceId = s"NativeShuffleReadExec:${UUID.randomUUID().toString}"
        val reader = SparkEnv.get.shuffleManager
          .getReader(
            shuffleHandle,
            partition.index,
            partition.index + 1,
            taskContext,
            metricReporter)
          .asInstanceOf[AuronBlockStoreShuffleReaderBase[_, _]]

        val ipcIterator = CompletionIterator[Object, Iterator[Object]](
          reader.readIpc(),
          taskContext.taskMetrics().mergeShuffleReadMetrics())
        JniBridge.putResource(jniResourceId, () => ipcIterator)

        PhysicalPlanNode
          .newBuilder()
          .setIpcReader(
            IpcReaderExecNode
              .newBuilder()
              .setSchema(nativeSchema)
              .setNumPartitions(rdd.getNumPartitions)
              .setIpcProviderResourceId(jniResourceId)
              .build())
          .build()
      },
      friendlyName = "NativeRDD.ShuffleRead")
  }

  def createNativeShuffleWriteProcessor(
      metrics: Map[String, SQLMetric],
      numPartitions: Int): ShuffleWriteProcessor

  def prepareNativeShuffleDependency(
      rdd: RDD[InternalRow],
      outputAttributes: Seq[Attribute],
      outputPartitioning: Partitioning,
      serializer: Serializer,
      metrics: Map[String, SQLMetric]): ShuffleDependency[Int, InternalRow, InternalRow] = {

    val nativeInputRDD = rdd.asInstanceOf[NativeRDD]
    val numPartitions = outputPartitioning.numPartitions
    val nativeMetrics = SparkMetricNode(
      metrics,
      nativeInputRDD.metrics :: Nil,
      Some({
        case ("elapsed_compute", v) => metrics("shuffle_write_total_time") += v
        case ("data_size", v) => metrics("dataSize") += v
        case ("output_rows", v) =>
          val shuffleWriteMetrics = TaskContext.get.taskMetrics().shuffleWriteMetrics
          new SQLShuffleWriteMetricsReporter(shuffleWriteMetrics, metrics).incRecordsWritten(v)
        case ("output_io_time", v) =>
          val shuffleWriteMetrics = TaskContext.get.taskMetrics().shuffleWriteMetrics
          new SQLShuffleWriteMetricsReporter(shuffleWriteMetrics, metrics).incWriteTime(v)
        case _ =>
      }))
    var numPartitionsRest = numPartitions;
    // if RangePartitioning => sample and find bounds
    val nativeBounds = outputPartitioning match {
      case RangePartitioning(sortingExpressions, numPartitions) =>
        // Extract only fields used for sorting to avoid collecting large fields that does not
        // affect sorting result when deciding partition bounds in RangePartitioner
        val rddForSampling = rdd.mapPartitionsInternal { iter =>
          val projection =
            UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
          val mutablePair = new MutablePair[InternalRow, Null]()
          // Internally, RangePartitioner runs a job on the RDD that samples keys to compute
          // partition bounds. To get accurate samples, we need to copy the mutable keys.
          iter.map(row => mutablePair.update(projection(row).copy(), null))
        }
        // Construct ordering on extracted sort key.
        val orderingAttributes = sortingExpressions.zipWithIndex.map { case (ord, i) =>
          ord.copy(child = BoundReference(i, ord.dataType, ord.nullable))
        }
        implicit val ordering = new LazilyGeneratedOrdering(orderingAttributes)

        // TODO: complete Native SampleExec
        val bounds = rangePartitioningBound(
          numPartitions,
          rddForSampling,
          samplePointsPerPartitionHint = SQLConf.get.rangeExchangeSampleSizePerPartition)
        numPartitionsRest = bounds.length + 1
        sortingExpressions.zipWithIndex.map { case (field, index) =>
          val valueList = bounds.map { internal_row =>
            internal_row.get(index, field.dataType)
          }
          val arrayData = ArrayData.toArrayData(valueList)
          NativeConverters.convertExpr(Literal(arrayData, ArrayType(field.dataType))).getLiteral
        }.toList

      case _ => null
    }

    val nativeShuffleRDD = new NativeRDD(
      nativeInputRDD.sparkContext,
      nativeMetrics,
      nativeInputRDD.partitions,
      nativeInputRDD.partitioner,
      new OneToOneDependency(nativeInputRDD) :: Nil,
      nativeInputRDD.isShuffleReadFull,
      (partition, taskContext) => {
        val nativeInputPartition = nativeInputRDD.partitions(partition.index)
        val repartitionBuilder = PhysicalRepartition.newBuilder()
        val nativeOutputPartitioning = outputPartitioning match {
          case SinglePartition =>
            repartitionBuilder
              .setSingleRepartition(
                PhysicalSingleRepartition
                  .newBuilder()
                  .setPartitionCount(1))
          case HashPartitioning(_, _) =>
            repartitionBuilder
              .setHashRepartition(
                PhysicalHashRepartition
                  .newBuilder()
                  .setPartitionCount(numPartitions)
                  .addAllHashExpr(nativeHashExprs.asJava))
          case RoundRobinPartitioning(_) =>
            repartitionBuilder
              .setRoundRobinRepartition(
                PhysicalRoundRobinRepartition
                  .newBuilder()
                  .setPartitionCount(numPartitions))
          case RangePartitioning(_, _) =>
            repartitionBuilder
              .setRangeRepartition(
                PhysicalRangeRepartition
                  .newBuilder()
                  .setPartitionCount(numPartitionsRest) // reset partition num
                  .addAllListValue(nativeBounds.asJava)
                  .setSortExpr(nativeSortExecNode))
          case p =>
            throw new NotImplementedError(s"cannot convert partitioning to native: $p")
        }

        val input = nativeInputRDD.nativePlan(nativeInputPartition, taskContext)
        val nativeShuffleWriteExec =
          Shims.get.getShuffleWriteExec(input, nativeOutputPartitioning)
        nativeShuffleWriteExec
      },
      friendlyName = "NativeRDD.ShuffleWrite")

    val dependency = new AuronShuffleDependency[Int, InternalRow, InternalRow](
      nativeShuffleRDD.map((0, _)),
      serializer = serializer,
      shuffleWriterProcessor = createNativeShuffleWriteProcessor(metrics, numPartitions),
      partitioner = new Partitioner {
        override def numPartitions: Int = numPartitionsRest

        override def getPartition(key: Any): Int = key.asInstanceOf[Int]
      },
      schema = Util.getSchema(outputAttributes, useExprId = false))
    metrics("numPartitions").set(numPartitionsRest)
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics("numPartitions") :: Nil)
    dependency
  }

  private def rangePartitioningBound[K: Ordering: ClassTag, V](
      partitions: Int,
      rdd: RDD[_ <: Product2[K, V]],
      samplePointsPerPartitionHint: Int = 20): Array[K] = {
    if (partitions <= 1) {
      Array.empty
    } else {
      // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
      // Cast to double to avoid overflowing ints or longs
      val sampleSize = math.min(samplePointsPerPartitionHint.toDouble * partitions, 1e6)
      // Assume the input partitions are roughly balanced and over-sample a little bit.
      val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.length).toInt
      val (numItems, sketched) = RangePartitioner.sketch(rdd.map(_._1), sampleSizePerPartition)
      if (numItems == 0L) {
        Array.empty
      } else {
        // If a partition contains much more than the average number of items, we re-sample from it
        // to ensure that enough items are collected from that partition.
        val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
        val candidates = ArrayBuffer.empty[(K, Float)]
        val imbalancedPartitions = mutable.Set.empty[Int]
        sketched.foreach { case (idx, n, sample) =>
          if (fraction * n > sampleSizePerPartition) {
            imbalancedPartitions += idx
          } else {
            // The weight is 1 over the sampling probability.
            val weight = (n.toDouble / sample.length).toFloat
            for (key <- sample) {
              candidates += ((key, weight))
            }
          }
        }
        if (imbalancedPartitions.nonEmpty) {
          // Re-sample imbalanced partitions with the desired sampling probability.
          val imbalanced = new PartitionPruningRDD(rdd.map(_._1), imbalancedPartitions.contains)
          val seed = byteswap32(-rdd.id - 1)
          val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
          val weight = (1.0 / fraction).toFloat
          candidates ++= reSampled.map(x => (x, weight))
        }
        RangePartitioner.determineBounds(candidates, math.min(partitions, candidates.size))
      }
    }
  }
}
