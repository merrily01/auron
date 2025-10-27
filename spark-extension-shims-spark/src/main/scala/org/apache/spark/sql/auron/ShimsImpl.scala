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
package org.apache.spark.sql.auron

import java.io.File
import java.util.UUID

import scala.collection.mutable

import org.apache.commons.lang3.reflect.FieldUtils
import org.apache.spark.{OneToOneDependency, ShuffleDependency, SparkContext, SparkEnv, SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.auron.AuronConverters.ForceNativeExecutionWrapperBase
import org.apache.spark.sql.auron.NativeConverters.NativeExprWrapperBase
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Generator
import org.apache.spark.sql.catalyst.expressions.Like
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.StringSplit
import org.apache.spark.sql.catalyst.expressions.TaggingExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.aggregate.First
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.CoalescedPartitionSpec
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.PartialMapperPartitionSpec
import org.apache.spark.sql.execution.PartialReducerPartitionSpec
import org.apache.spark.sql.execution.ShuffledRowRDD
import org.apache.spark.sql.execution.ShufflePartitionSpec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.auron.plan._
import org.apache.spark.sql.execution.auron.plan.ConvertToNativeExec
import org.apache.spark.sql.execution.auron.plan.NativeAggBase
import org.apache.spark.sql.execution.auron.plan.NativeAggBase.AggExecMode
import org.apache.spark.sql.execution.auron.plan.NativeAggExec
import org.apache.spark.sql.execution.auron.plan.NativeBroadcastExchangeBase
import org.apache.spark.sql.execution.auron.plan.NativeBroadcastExchangeExec
import org.apache.spark.sql.execution.auron.plan.NativeExpandBase
import org.apache.spark.sql.execution.auron.plan.NativeExpandExec
import org.apache.spark.sql.execution.auron.plan.NativeFilterBase
import org.apache.spark.sql.execution.auron.plan.NativeFilterExec
import org.apache.spark.sql.execution.auron.plan.NativeGenerateBase
import org.apache.spark.sql.execution.auron.plan.NativeGenerateExec
import org.apache.spark.sql.execution.auron.plan.NativeGlobalLimitBase
import org.apache.spark.sql.execution.auron.plan.NativeGlobalLimitExec
import org.apache.spark.sql.execution.auron.plan.NativeLocalLimitBase
import org.apache.spark.sql.execution.auron.plan.NativeLocalLimitExec
import org.apache.spark.sql.execution.auron.plan.NativeOrcScanExec
import org.apache.spark.sql.execution.auron.plan.NativeParquetInsertIntoHiveTableBase
import org.apache.spark.sql.execution.auron.plan.NativeParquetInsertIntoHiveTableExec
import org.apache.spark.sql.execution.auron.plan.NativeParquetScanBase
import org.apache.spark.sql.execution.auron.plan.NativeParquetScanExec
import org.apache.spark.sql.execution.auron.plan.NativeProjectBase
import org.apache.spark.sql.execution.auron.plan.NativeRenameColumnsBase
import org.apache.spark.sql.execution.auron.plan.NativeShuffleExchangeBase
import org.apache.spark.sql.execution.auron.plan.NativeShuffleExchangeExec
import org.apache.spark.sql.execution.auron.plan.NativeSortBase
import org.apache.spark.sql.execution.auron.plan.NativeSortExec
import org.apache.spark.sql.execution.auron.plan.NativeTakeOrderedBase
import org.apache.spark.sql.execution.auron.plan.NativeTakeOrderedExec
import org.apache.spark.sql.execution.auron.plan.NativeUnionBase
import org.apache.spark.sql.execution.auron.plan.NativeUnionExec
import org.apache.spark.sql.execution.auron.plan.NativeWindowBase
import org.apache.spark.sql.execution.auron.plan.NativeWindowExec
import org.apache.spark.sql.execution.auron.shuffle.{AuronBlockStoreShuffleReaderBase, AuronRssShuffleManagerBase, RssPartitionWriterBase}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.auron.plan.NativeBroadcastJoinExec
import org.apache.spark.sql.execution.joins.auron.plan.NativeShuffledHashJoinExecProvider
import org.apache.spark.sql.execution.joins.auron.plan.NativeSortMergeJoinExecProvider
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLShuffleReadMetricsReporter}
import org.apache.spark.sql.execution.ui.{AuronEventUtils, AuronSQLAppStatusListener, AuronSQLAppStatusStore, AuronSQLTab}
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.storage.FileSegment

import org.apache.auron.{protobuf => pb, sparkver}
import org.apache.auron.common.AuronBuildInfo
import org.apache.auron.metric.SparkMetricNode
import org.apache.auron.spark.ui.AuronBuildInfoEvent

class ShimsImpl extends Shims with Logging {

  @sparkver("3.0")
  override def shimVersion: String = "spark-3.0"
  @sparkver("3.1")
  override def shimVersion: String = "spark-3.1"
  @sparkver("3.2")
  override def shimVersion: String = "spark-3.2"
  @sparkver("3.3")
  override def shimVersion: String = "spark-3.3"
  @sparkver("3.4")
  override def shimVersion: String = "spark-3.4"
  @sparkver("3.5")
  override def shimVersion: String = "spark-3.5"

  @sparkver("3.2 / 3.3 / 3.4 / 3.5")
  override def initExtension(): Unit = {
    ValidateSparkPlanInjector.inject()

    if (AuronConf.FORCE_SHUFFLED_HASH_JOIN.booleanConf()) {
      ForceApplyShuffledHashJoinInjector.inject()
    }

    // disable MultiCommutativeOp suggested in spark3.4+
    if (shimVersion >= "spark-3.4") {
      val confName = "spark.sql.analyzer.canonicalization.multiCommutativeOpMemoryOptThreshold"
      SparkEnv.get.conf.set(confName, Int.MaxValue.toString)
    }
  }

  @sparkver("3.0 / 3.1")
  override def initExtension(): Unit = {
    if (AuronConf.FORCE_SHUFFLED_HASH_JOIN.booleanConf()) {
      logWarning(s"${AuronConf.FORCE_SHUFFLED_HASH_JOIN.key} is not supported in $shimVersion")
    }

  }

  // set Auron spark ui if spark.auron.ui.enabled is true
  override def onApplyingExtension(): Unit = {
    logInfo(
      " onApplyingExtension get ui_enabled : " + SparkEnv.get.conf
        .get(AuronConf.UI_ENABLED.key, "true"))

    if (SparkEnv.get.conf.get(AuronConf.UI_ENABLED.key, "true").equals("true")) {
      val sparkContext = SparkContext.getActive.getOrElse {
        throw new IllegalStateException("No active spark context found that should not happen")
      }
      val kvStore = sparkContext.statusStore.store.asInstanceOf[ElementTrackingStore]
      val statusStore = new AuronSQLAppStatusStore(kvStore)
      sparkContext.ui.foreach(new AuronSQLTab(statusStore, _))
      logInfo(" onApplyingExtension add register ")
      AuronSQLAppStatusListener.register(sparkContext)
      postBuildInfoEvent(sparkContext)
    }

  }

  private def postBuildInfoEvent(sparkContext: SparkContext): Unit = {
    val auronBuildInfo = new mutable.LinkedHashMap[String, String]()
    auronBuildInfo.put(AuronBuildInfo.VERSION_STRING, AuronBuildInfo.VERSION)
    auronBuildInfo.put(
      AuronBuildInfo.JAVA_COMPILE_VERSION_STRING,
      AuronBuildInfo.JAVA_COMPILE_VERSION)
    auronBuildInfo.put(
      AuronBuildInfo.SCALA_COMPILE_VERSION_STRING,
      AuronBuildInfo.SCALA_COMPILE_VERSION)
    auronBuildInfo.put(
      AuronBuildInfo.SPARK_COMPILE_VERSION_STRING,
      AuronBuildInfo.SPARK_COMPILE_VERSION)
    auronBuildInfo.put(
      AuronBuildInfo.RUST_COMPILE_VERSION_STRING,
      AuronBuildInfo.RUST_COMPILE_VERSION)
    auronBuildInfo.put(AuronBuildInfo.CELEBORN_VERSION_STRING, AuronBuildInfo.CELEBORN_VERSION)
    auronBuildInfo.put(AuronBuildInfo.UNIFFLE_VERSION_STRING, AuronBuildInfo.UNIFFLE_VERSION)
    auronBuildInfo.put(AuronBuildInfo.PAIMON_VERSION_STRING, AuronBuildInfo.PAIMON_VERSION)
    auronBuildInfo.put(AuronBuildInfo.FLINK_VERSION_STRING, AuronBuildInfo.FLINK_VERSION)
    auronBuildInfo.put(AuronBuildInfo.BUILD_DATE_STRING, AuronBuildInfo.BUILD_DATE)
    val event = AuronBuildInfoEvent(auronBuildInfo)
    AuronEventUtils.post(sparkContext, event)
  }

  override def createConvertToNativeExec(child: SparkPlan): ConvertToNativeBase =
    ConvertToNativeExec(child)

  override def createNativeAggExec(
      execMode: AggExecMode,
      requiredChildDistributionExpressions: Option[Seq[Expression]],
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute],
      initialInputBufferOffset: Int,
      child: SparkPlan): NativeAggBase =
    NativeAggExec(
      execMode,
      requiredChildDistributionExpressions,
      groupingExpressions,
      aggregateExpressions,
      aggregateAttributes,
      initialInputBufferOffset,
      child)

  override def createNativeBroadcastExchangeExec(
      mode: BroadcastMode,
      child: SparkPlan): NativeBroadcastExchangeBase =
    NativeBroadcastExchangeExec(mode, child)

  override def createNativeBroadcastJoinExec(
      left: SparkPlan,
      right: SparkPlan,
      outputPartitioning: Partitioning,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      broadcastSide: BroadcastSide): NativeBroadcastJoinBase =
    NativeBroadcastJoinExec(
      left,
      right,
      outputPartitioning,
      leftKeys,
      rightKeys,
      joinType,
      broadcastSide)

  override def createNativeSortMergeJoinExec(
      left: SparkPlan,
      right: SparkPlan,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      isSkewJoin: Boolean): NativeSortMergeJoinBase =
    NativeSortMergeJoinExecProvider.provide(
      left,
      right,
      leftKeys,
      rightKeys,
      joinType,
      isSkewJoin)

  override def createNativeShuffledHashJoinExec(
      left: SparkPlan,
      right: SparkPlan,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      isSkewJoin: Boolean): SparkPlan =
    NativeShuffledHashJoinExecProvider.provide(
      left,
      right,
      leftKeys,
      rightKeys,
      joinType,
      buildSide,
      isSkewJoin)

  override def createNativeExpandExec(
      projections: Seq[Seq[Expression]],
      output: Seq[Attribute],
      child: SparkPlan): NativeExpandBase =
    NativeExpandExec(projections, output, child)

  override def createNativeFilterExec(condition: Expression, child: SparkPlan): NativeFilterBase =
    NativeFilterExec(condition, child)

  override def createNativeGenerateExec(
      generator: Generator,
      requiredChildOutput: Seq[Attribute],
      outer: Boolean,
      generatorOutput: Seq[Attribute],
      child: SparkPlan): NativeGenerateBase =
    NativeGenerateExec(generator, requiredChildOutput, outer, generatorOutput, child)

  override def createNativeGlobalLimitExec(limit: Long, child: SparkPlan): NativeGlobalLimitBase =
    NativeGlobalLimitExec(limit, child)

  override def createNativeLocalLimitExec(limit: Long, child: SparkPlan): NativeLocalLimitBase =
    NativeLocalLimitExec(limit, child)

  override def createNativeParquetInsertIntoHiveTableExec(
      cmd: InsertIntoHiveTable,
      child: SparkPlan): NativeParquetInsertIntoHiveTableBase =
    NativeParquetInsertIntoHiveTableExec(cmd, child)

  override def createNativeParquetScanExec(
      basedFileScan: FileSourceScanExec): NativeParquetScanBase =
    NativeParquetScanExec(basedFileScan)

  override def createNativeOrcScanExec(basedFileScan: FileSourceScanExec): NativeOrcScanBase =
    NativeOrcScanExec(basedFileScan)

  override def createNativeProjectExec(
      projectList: Seq[NamedExpression],
      child: SparkPlan): NativeProjectBase =
    NativeProjectExecProvider.provide(projectList, child)

  override def createNativeRenameColumnsExec(
      child: SparkPlan,
      newColumnNames: Seq[String]): NativeRenameColumnsBase =
    NativeRenameColumnsExecProvider.provide(child, newColumnNames)

  override def createNativeShuffleExchangeExec(
      outputPartitioning: Partitioning,
      child: SparkPlan,
      shuffleOrigin: Option[Any] = None): NativeShuffleExchangeBase =
    NativeShuffleExchangeExec(outputPartitioning, child, shuffleOrigin)

  override def createNativeSortExec(
      sortOrder: Seq[SortOrder],
      global: Boolean,
      child: SparkPlan): NativeSortBase =
    NativeSortExec(sortOrder, global, child)

  override def createNativeTakeOrderedExec(
      limit: Long,
      sortOrder: Seq[SortOrder],
      child: SparkPlan): NativeTakeOrderedBase =
    NativeTakeOrderedExec(limit, sortOrder, child)

  override def createNativePartialTakeOrderedExec(
      limit: Long,
      sortOrder: Seq[SortOrder],
      child: SparkPlan,
      metrics: Map[String, SQLMetric]): NativePartialTakeOrderedBase =
    NativePartialTakeOrderedExec(limit, sortOrder, child, metrics)

  override def createNativeUnionExec(
      children: Seq[SparkPlan],
      output: Seq[Attribute]): NativeUnionBase =
    NativeUnionExec(children, output)

  override def createNativeWindowExec(
      windowExpression: Seq[NamedExpression],
      partitionSpec: Seq[Expression],
      orderSpec: Seq[SortOrder],
      groupLimit: Option[Int],
      outputWindowCols: Boolean,
      child: SparkPlan): NativeWindowBase =
    NativeWindowExec(windowExpression, partitionSpec, orderSpec, groupLimit, child)

  override def createNativeParquetSinkExec(
      sparkSession: SparkSession,
      table: CatalogTable,
      partition: Map[String, Option[String]],
      child: SparkPlan,
      metrics: Map[String, SQLMetric]): NativeParquetSinkBase =
    NativeParquetSinkExec(sparkSession, table, partition, child, metrics)

  override def getUnderlyingBroadcast(plan: SparkPlan): BroadcastExchangeLike = {
    plan match {
      case exec: BroadcastExchangeLike => exec
      case exec: UnaryExecNode => getUnderlyingBroadcast(exec.child)
      case exec: BroadcastQueryStageExec => getUnderlyingBroadcast(exec.broadcast)
      case exec: ReusedExchangeExec => getUnderlyingBroadcast(exec.child)
    }
  }

  override def isNative(plan: SparkPlan): Boolean =
    plan match {
      case _: NativeSupports => true
      case plan if isAQEShuffleRead(plan) => isNative(plan.children.head)
      case plan: QueryStageExec => isNative(plan.plan)
      case plan: ReusedExchangeExec => isNative(plan.child)
      case _ => false
    }

  override def getUnderlyingNativePlan(plan: SparkPlan): NativeSupports = {
    plan match {
      case plan: NativeSupports => plan
      case plan if isAQEShuffleRead(plan) => getUnderlyingNativePlan(plan.children.head)
      case plan: QueryStageExec => getUnderlyingNativePlan(plan.plan)
      case plan: ReusedExchangeExec => getUnderlyingNativePlan(plan.child)
      case _ => throw new RuntimeException("unreachable: plan is not native")
    }
  }

  override def executeNative(plan: SparkPlan): NativeRDD = {
    plan match {
      case plan: NativeSupports => plan.executeNative()
      case plan if isAQEShuffleRead(plan) => executeNativeAQEShuffleReader(plan)
      case plan: QueryStageExec => executeNative(plan.plan)
      case plan: ReusedExchangeExec => executeNative(plan.child)
      case _ =>
        throw new SparkException(s"Underlying plan is not NativeSupports: ${plan}")
    }
  }

  override def isQueryStageInput(plan: SparkPlan): Boolean = {
    plan.isInstanceOf[QueryStageExec]
  }

  override def isShuffleQueryStageInput(plan: SparkPlan): Boolean = {
    plan.isInstanceOf[ShuffleQueryStageExec]
  }

  override def getChildStage(plan: SparkPlan): SparkPlan =
    plan.asInstanceOf[QueryStageExec].plan

  override def simpleStringWithNodeId(plan: SparkPlan): String = plan.simpleStringWithNodeId()

  override def setLogicalLink(exec: SparkPlan, basedExec: SparkPlan): SparkPlan = {
    basedExec.logicalLink.foreach(logicalLink => exec.setLogicalLink(logicalLink))
    exec
  }

  override def getRDDShuffleReadFull(rdd: RDD[_]): Boolean = true

  override def setRDDShuffleReadFull(rdd: RDD[_], shuffleReadFull: Boolean): Unit = {}

  override def createFileSegment(
      file: File,
      offset: Long,
      length: Long,
      numRecords: Long): FileSegment = new FileSegment(file, offset, length)

  @sparkver("3.2 / 3.3 / 3.4 / 3.5")
  override def commit(
      dep: ShuffleDependency[_, _, _],
      shuffleBlockResolver: IndexShuffleBlockResolver,
      tempDataFile: File,
      mapId: Long,
      partitionLengths: Array[Long],
      dataSize: Long,
      context: TaskContext): MapStatus = {

    val checksums = Array[Long]()
    shuffleBlockResolver.writeMetadataFileAndCommit(
      dep.shuffleId,
      mapId,
      partitionLengths,
      checksums,
      tempDataFile)
    MapStatus.apply(SparkEnv.get.blockManager.shuffleServerId, partitionLengths, mapId)
  }

  @sparkver("3.0 / 3.1")
  override def commit(
      dep: ShuffleDependency[_, _, _],
      shuffleBlockResolver: IndexShuffleBlockResolver,
      tempDataFile: File,
      mapId: Long,
      partitionLengths: Array[Long],
      dataSize: Long,
      context: TaskContext): MapStatus = {

    shuffleBlockResolver.writeIndexFileAndCommit(
      dep.shuffleId,
      mapId,
      partitionLengths,
      tempDataFile)
    MapStatus.apply(SparkEnv.get.blockManager.shuffleServerId, partitionLengths, mapId)
  }

  override def getRssPartitionWriter(
      handle: ShuffleHandle,
      mapId: Int,
      metrics: ShuffleWriteMetricsReporter,
      numPartitions: Int): Option[RssPartitionWriterBase] = None

  override def getMapStatus(
      shuffleServerId: BlockManagerId,
      partitionLengthMap: Array[Long],
      mapId: Long): MapStatus =
    MapStatus.apply(shuffleServerId, partitionLengthMap, mapId)

  override def getShuffleWriteExec(
      input: pb.PhysicalPlanNode,
      nativeOutputPartitioning: pb.PhysicalRepartition.Builder): pb.PhysicalPlanNode = {

    if (SparkEnv.get.shuffleManager.isInstanceOf[AuronRssShuffleManagerBase]) {
      return pb.PhysicalPlanNode
        .newBuilder()
        .setRssShuffleWriter(
          pb.RssShuffleWriterExecNode
            .newBuilder()
            .setInput(input)
            .setOutputPartitioning(nativeOutputPartitioning)
            .buildPartial()
        ) // shuffleId is not set at the moment, will be set in ShuffleWriteProcessor
        .build()
    }

    pb.PhysicalPlanNode
      .newBuilder()
      .setShuffleWriter(
        pb.ShuffleWriterExecNode
          .newBuilder()
          .setInput(input)
          .setOutputPartitioning(nativeOutputPartitioning)
          .buildPartial()
      ) // shuffleId is not set at the moment, will be set in ShuffleWriteProcessor
      .build()
  }

  override def convertMoreExprWithFallback(
      e: Expression,
      isPruningExpr: Boolean,
      fallback: Expression => pb.PhysicalExprNode): Option[pb.PhysicalExprNode] = {
    e match {
      case StringSplit(str, pat @ Literal(_, StringType), Literal(-1, IntegerType))
          // native StringSplit implementation does not support regex, so only most frequently
          // used cases without regex are supported
          if Seq(",", ", ", ":", ";", "#", "@", "_", "-", "\\|", "\\.").contains(pat.value) =>
        val nativePat = pat.value match {
          case "\\|" => "|"
          case "\\." => "."
          case other => other
        }
        Some(
          pb.PhysicalExprNode
            .newBuilder()
            .setScalarFunction(
              pb.PhysicalScalarFunctionNode
                .newBuilder()
                .setFun(pb.ScalarFunction.SparkExtFunctions)
                .setName("StringSplit")
                .addArgs(NativeConverters.convertExprWithFallback(str, isPruningExpr, fallback))
                .addArgs(NativeConverters
                  .convertExprWithFallback(Literal(nativePat), isPruningExpr, fallback))
                .setReturnType(NativeConverters.convertDataType(StringType)))
            .build())

      case e: TaggingExpression =>
        Some(NativeConverters.convertExprWithFallback(e.child, isPruningExpr, fallback))
      case e =>
        convertPromotePrecision(e, isPruningExpr, fallback) match {
          case Some(v) => return Some(v)
          case None =>
        }
        convertBloomFilterMightContain(e, isPruningExpr, fallback) match {
          case Some(v) => return Some(v)
          case None =>
        }
        None
    }
  }

  override def getLikeEscapeChar(expr: Expression): Char = {
    expr.asInstanceOf[Like].escapeChar
  }

  override def convertMoreAggregateExpr(e: AggregateExpression): Option[pb.PhysicalExprNode] = {
    assert(getAggregateExpressionFilter(e).isEmpty)

    e.aggregateFunction match {
      case First(child, ignoresNull) =>
        val aggExpr = pb.PhysicalAggExprNode
          .newBuilder()
          .setReturnType(NativeConverters.convertDataType(e.dataType))
          .setAggFunction(if (ignoresNull) {
            pb.AggFunction.FIRST_IGNORES_NULL
          } else {
            pb.AggFunction.FIRST
          })
          .addChildren(NativeConverters.convertExpr(child))
        Some(pb.PhysicalExprNode.newBuilder().setAggExpr(aggExpr).build())

      case agg =>
        convertBloomFilterAgg(agg) match {
          case Some(aggExpr) =>
            return Some(
              pb.PhysicalExprNode
                .newBuilder()
                .setAggExpr(aggExpr)
                .build())
          case None =>
        }
        None
    }
  }

  override def getAggregateExpressionFilter(expr: Expression): Option[Expression] = {
    expr.asInstanceOf[AggregateExpression].filter
  }

  @sparkver("3.2 / 3.3 / 3.4 / 3.5")
  private def isAQEShuffleRead(exec: SparkPlan): Boolean = {
    import org.apache.spark.sql.execution.adaptive.AQEShuffleReadExec
    exec.isInstanceOf[AQEShuffleReadExec]
  }

  @sparkver("3.0 / 3.1")
  private def isAQEShuffleRead(exec: SparkPlan): Boolean = {
    import org.apache.spark.sql.execution.adaptive.CustomShuffleReaderExec
    exec.isInstanceOf[CustomShuffleReaderExec]
  }

  @sparkver("3.2 / 3.3 / 3.4 / 3.5")
  private def executeNativeAQEShuffleReader(exec: SparkPlan): NativeRDD = {
    import org.apache.spark.sql.execution.adaptive.AQEShuffleReadExec
    import org.apache.spark.sql.execution.CoalescedMapperPartitionSpec

    exec match {
      case AQEShuffleReadExec(child, _) if isNative(child) =>
        val shuffledRDD = exec.execute().asInstanceOf[ShuffledRowRDD]
        val shuffleHandle = shuffledRDD.dependency.shuffleHandle

        val inputRDD = executeNative(child)
        val nativeShuffle = getUnderlyingNativePlan(child).asInstanceOf[NativeShuffleExchangeExec]
        val nativeSchema: pb.Schema = nativeShuffle.nativeSchema

        val requiredMetrics = nativeShuffle.readMetrics ++
          nativeShuffle.metrics.filterKeys(_ == "shuffle_read_total_time")
        val metrics = SparkMetricNode(
          requiredMetrics,
          inputRDD.metrics :: Nil,
          Some({
            case ("output_rows", v) =>
              val tempMetrics = TaskContext.get.taskMetrics().createTempShuffleReadMetrics()
              new SQLShuffleReadMetricsReporter(tempMetrics, requiredMetrics).incRecordsRead(v)
              TaskContext.get().taskMetrics().mergeShuffleReadMetrics()
            case ("elapsed_compute", v) => requiredMetrics("shuffle_read_total_time") += v
            case _ =>
          }))

        new NativeRDD(
          shuffledRDD.sparkContext,
          metrics,
          shuffledRDD.partitions,
          shuffledRDD.partitioner,
          new OneToOneDependency(shuffledRDD) :: Nil,
          true,
          (partition, taskContext) => {

            // use reflection to get partitionSpec because ShuffledRowRDDPartition is private
            val sqlMetricsReporter = taskContext.taskMetrics().createTempShuffleReadMetrics()
            val spec = FieldUtils
              .readDeclaredField(partition, "spec", true)
              .asInstanceOf[ShufflePartitionSpec]
            val reader = spec match {
              case CoalescedPartitionSpec(startReducerIndex, endReducerIndex, _) =>
                SparkEnv.get.shuffleManager.getReader(
                  shuffleHandle,
                  startReducerIndex,
                  endReducerIndex,
                  taskContext,
                  sqlMetricsReporter)

              case CoalescedMapperPartitionSpec(startMapIndex, endMapIndex, numReducers) =>
                SparkEnv.get.shuffleManager.getReader(
                  shuffleHandle,
                  startMapIndex,
                  endMapIndex,
                  0,
                  numReducers,
                  taskContext,
                  sqlMetricsReporter)

              case PartialReducerPartitionSpec(reducerIndex, startMapIndex, endMapIndex, _) =>
                SparkEnv.get.shuffleManager.getReader(
                  shuffleHandle,
                  startMapIndex,
                  endMapIndex,
                  reducerIndex,
                  reducerIndex + 1,
                  taskContext,
                  sqlMetricsReporter)

              case PartialMapperPartitionSpec(mapIndex, startReducerIndex, endReducerIndex) =>
                SparkEnv.get.shuffleManager.getReader(
                  shuffleHandle,
                  mapIndex,
                  mapIndex + 1,
                  startReducerIndex,
                  endReducerIndex,
                  taskContext,
                  sqlMetricsReporter)
            }

            // store fetch iterator in jni resource before native compute
            val jniResourceId = s"NativeShuffleReadExec:${UUID.randomUUID().toString}"
            org.apache.auron.jni.JniBridge.putResource(
              jniResourceId,
              () => {
                reader.asInstanceOf[AuronBlockStoreShuffleReaderBase[_, _]].readIpc()
              })

            pb.PhysicalPlanNode
              .newBuilder()
              .setIpcReader(
                pb.IpcReaderExecNode
                  .newBuilder()
                  .setSchema(nativeSchema)
                  .setNumPartitions(shuffledRDD.getNumPartitions)
                  .setIpcProviderResourceId(jniResourceId)
                  .build())
              .build()
          })
    }
  }

  @sparkver("3.1")
  private def executeNativeAQEShuffleReader(exec: SparkPlan): NativeRDD = {
    import org.apache.spark.sql.execution.adaptive.CustomShuffleReaderExec

    exec match {
      case CustomShuffleReaderExec(child, _) if isNative(child) =>
        val shuffledRDD = exec.execute().asInstanceOf[ShuffledRowRDD]
        val shuffleHandle = shuffledRDD.dependency.shuffleHandle

        val inputRDD = executeNative(child)
        val nativeShuffle = getUnderlyingNativePlan(child).asInstanceOf[NativeShuffleExchangeExec]
        val nativeSchema: pb.Schema = nativeShuffle.nativeSchema

        val requiredMetrics = nativeShuffle.readMetrics ++
          nativeShuffle.metrics.filterKeys(_ == "shuffle_read_total_time")
        val metrics = SparkMetricNode(
          requiredMetrics,
          inputRDD.metrics :: Nil,
          Some({
            case ("output_rows", v) =>
              val tempMetrics = TaskContext.get.taskMetrics().createTempShuffleReadMetrics()
              new SQLShuffleReadMetricsReporter(tempMetrics, requiredMetrics).incRecordsRead(v)
              TaskContext.get().taskMetrics().mergeShuffleReadMetrics()
            case ("elapsed_compute", v) => requiredMetrics("shuffle_read_total_time") += v
            case _ =>
          }))

        new NativeRDD(
          shuffledRDD.sparkContext,
          metrics,
          shuffledRDD.partitions,
          shuffledRDD.partitioner,
          new OneToOneDependency(shuffledRDD) :: Nil,
          true,
          (partition, taskContext) => {

            // use reflection to get partitionSpec because ShuffledRowRDDPartition is private
            val sqlMetricsReporter = taskContext.taskMetrics().createTempShuffleReadMetrics()
            val spec = FieldUtils
              .readDeclaredField(partition, "spec", true)
              .asInstanceOf[ShufflePartitionSpec]
            val reader = spec match {
              case CoalescedPartitionSpec(startReducerIndex, endReducerIndex) =>
                SparkEnv.get.shuffleManager.getReader(
                  shuffleHandle,
                  startReducerIndex,
                  endReducerIndex,
                  taskContext,
                  sqlMetricsReporter)

              case PartialReducerPartitionSpec(reducerIndex, startMapIndex, endMapIndex, _) =>
                SparkEnv.get.shuffleManager.getReader(
                  shuffleHandle,
                  startMapIndex,
                  endMapIndex,
                  reducerIndex,
                  reducerIndex + 1,
                  taskContext,
                  sqlMetricsReporter)

              case PartialMapperPartitionSpec(mapIndex, startReducerIndex, endReducerIndex) =>
                SparkEnv.get.shuffleManager.getReader(
                  shuffleHandle,
                  mapIndex,
                  mapIndex + 1,
                  startReducerIndex,
                  endReducerIndex,
                  taskContext,
                  sqlMetricsReporter)
            }

            // store fetch iterator in jni resource before native compute
            val jniResourceId = s"NativeShuffleReadExec:${UUID.randomUUID().toString}"
            org.apache.auron.jni.JniBridge.putResource(
              jniResourceId,
              () => {
                reader.asInstanceOf[AuronBlockStoreShuffleReaderBase[_, _]].readIpc()
              })

            pb.PhysicalPlanNode
              .newBuilder()
              .setIpcReader(
                pb.IpcReaderExecNode
                  .newBuilder()
                  .setSchema(nativeSchema)
                  .setNumPartitions(shuffledRDD.getNumPartitions)
                  .setIpcProviderResourceId(jniResourceId)
                  .build())
              .build()
          })
    }
  }

  @sparkver("3.0")
  private def executeNativeAQEShuffleReader(exec: SparkPlan): NativeRDD = {
    import org.apache.spark.sql.execution.adaptive.CustomShuffleReaderExec

    exec match {
      case CustomShuffleReaderExec(child, _, _) if isNative(child) =>
        val shuffledRDD = exec.execute().asInstanceOf[ShuffledRowRDD]
        val shuffleHandle = shuffledRDD.dependency.shuffleHandle

        val inputRDD = executeNative(child)
        val nativeShuffle = getUnderlyingNativePlan(child).asInstanceOf[NativeShuffleExchangeExec]
        val nativeSchema: pb.Schema = nativeShuffle.nativeSchema

        val requiredMetrics = nativeShuffle.readMetrics ++
          nativeShuffle.metrics.filterKeys(_ == "shuffle_read_total_time")
        val metrics = SparkMetricNode(
          requiredMetrics,
          inputRDD.metrics :: Nil,
          Some({
            case ("output_rows", v) =>
              val tempMetrics = TaskContext.get.taskMetrics().createTempShuffleReadMetrics()
              new SQLShuffleReadMetricsReporter(tempMetrics, requiredMetrics).incRecordsRead(v)
              TaskContext.get().taskMetrics().mergeShuffleReadMetrics()
            case ("elapsed_compute", v) => requiredMetrics("shuffle_read_total_time") += v
            case _ =>
          }))

        new NativeRDD(
          shuffledRDD.sparkContext,
          metrics,
          shuffledRDD.partitions,
          shuffledRDD.partitioner,
          new OneToOneDependency(shuffledRDD) :: Nil,
          true,
          (partition, taskContext) => {

            // use reflection to get partitionSpec because ShuffledRowRDDPartition is private
            val sqlMetricsReporter = taskContext.taskMetrics().createTempShuffleReadMetrics()
            val spec = FieldUtils
              .readDeclaredField(partition, "spec", true)
              .asInstanceOf[ShufflePartitionSpec]
            val reader = spec match {
              case CoalescedPartitionSpec(startReducerIndex, endReducerIndex) =>
                SparkEnv.get.shuffleManager.getReader(
                  shuffleHandle,
                  startReducerIndex,
                  endReducerIndex,
                  taskContext,
                  sqlMetricsReporter)

              case PartialReducerPartitionSpec(reducerIndex, startMapIndex, endMapIndex) =>
                SparkEnv.get.shuffleManager.getReaderForRange(
                  shuffleHandle,
                  startMapIndex,
                  endMapIndex,
                  reducerIndex,
                  reducerIndex + 1,
                  taskContext,
                  sqlMetricsReporter)

              case PartialMapperPartitionSpec(mapIndex, startReducerIndex, endReducerIndex) =>
                SparkEnv.get.shuffleManager.getReaderForRange(
                  shuffleHandle,
                  mapIndex,
                  mapIndex + 1,
                  startReducerIndex,
                  endReducerIndex,
                  taskContext,
                  sqlMetricsReporter)
            }

            // store fetch iterator in jni resource before native compute
            val jniResourceId = s"NativeShuffleReadExec:${UUID.randomUUID().toString}"
            org.apache.auron.jni.JniBridge.putResource(
              jniResourceId,
              () => {
                reader.asInstanceOf[AuronBlockStoreShuffleReaderBase[_, _]].readIpc()
              })

            pb.PhysicalPlanNode
              .newBuilder()
              .setIpcReader(
                pb.IpcReaderExecNode
                  .newBuilder()
                  .setSchema(nativeSchema)
                  .setNumPartitions(shuffledRDD.getNumPartitions)
                  .setIpcProviderResourceId(jniResourceId)
                  .build())
              .build()
          })
    }
  }

  override def convertMoreSparkPlan(exec: SparkPlan): Option[SparkPlan] = {
    exec match {
      case exec if isAQEShuffleRead(exec) && isNative(exec) =>
        Some(ForceNativeExecutionWrapper(AuronConverters.addRenameColumnsExec(exec)))
      case _: ReusedExchangeExec if isNative(exec) =>
        Some(ForceNativeExecutionWrapper(AuronConverters.addRenameColumnsExec(exec)))
      case _ => None
    }
  }

  @sparkver("3.2 / 3.3 / 3.4 / 3.5")
  override def getSqlContext(sparkPlan: SparkPlan): SQLContext =
    sparkPlan.session.sqlContext

  @sparkver("3.0 / 3.1")
  override def getSqlContext(sparkPlan: SparkPlan): SQLContext = sparkPlan.sqlContext

  override def createNativeExprWrapper(
      nativeExpr: pb.PhysicalExprNode,
      dataType: DataType,
      nullable: Boolean): Expression = {
    NativeExprWrapper(nativeExpr, dataType, nullable)
  }

  @sparkver("3.0 / 3.1 / 3.2 / 3.3")
  override def getPartitionedFile(
      partitionValues: InternalRow,
      filePath: String,
      offset: Long,
      size: Long): PartitionedFile =
    PartitionedFile(partitionValues, filePath, offset, size)

  @sparkver("3.4 / 3.5")
  override def getPartitionedFile(
      partitionValues: InternalRow,
      filePath: String,
      offset: Long,
      size: Long): PartitionedFile = {
    import org.apache.hadoop.fs.Path
    import org.apache.spark.paths.SparkPath
    PartitionedFile(partitionValues, SparkPath.fromPath(new Path(filePath)), offset, size)
  }

  @sparkver("3.1 / 3.2 / 3.3 / 3.4 / 3.5")
  override def getMinPartitionNum(sparkSession: SparkSession): Int =
    sparkSession.sessionState.conf.filesMinPartitionNum
      .getOrElse(sparkSession.sparkContext.defaultParallelism)

  @sparkver("3.0")
  override def getMinPartitionNum(sparkSession: SparkSession): Int =
    sparkSession.sparkContext.defaultParallelism

  @sparkver("3.0 / 3.1 / 3.2 / 3.3")
  private def convertPromotePrecision(
      e: Expression,
      isPruningExpr: Boolean,
      fallback: Expression => pb.PhysicalExprNode): Option[pb.PhysicalExprNode] = {
    import org.apache.spark.sql.catalyst.expressions.PromotePrecision
    e match {
      case PromotePrecision(_1) if NativeConverters.decimalArithOpEnabled =>
        Some(NativeConverters.convertExprWithFallback(_1, isPruningExpr, fallback))
      case _ => None
    }
  }

  @sparkver("3.4 / 3.5")
  private def convertPromotePrecision(
      e: Expression,
      isPruningExpr: Boolean,
      fallback: Expression => pb.PhysicalExprNode): Option[pb.PhysicalExprNode] = None

  @sparkver("3.3 / 3.4 / 3.5")
  private def convertBloomFilterAgg(agg: AggregateFunction): Option[pb.PhysicalAggExprNode] = {
    import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
    agg match {
      case BloomFilterAggregate(child, estimatedNumItemsExpression, numBitsExpression, _, _) =>
        // ensure numBits is a power of 2
        val estimatedNumItems =
          estimatedNumItemsExpression.eval().asInstanceOf[Number].longValue()
        val numBits = numBitsExpression.eval().asInstanceOf[Number].longValue()
        val numBitsNextPowerOf2 = numBits match {
          case 1 => 1L
          case n => Integer.highestOneBit(n.toInt - 1) << 1
        }
        Some(
          pb.PhysicalAggExprNode
            .newBuilder()
            .setReturnType(NativeConverters.convertDataType(agg.dataType))
            .setAggFunction(pb.AggFunction.BLOOM_FILTER)
            .addChildren(NativeConverters.convertExpr(child))
            .addChildren(NativeConverters.convertExpr(Literal(estimatedNumItems)))
            .addChildren(NativeConverters.convertExpr(Literal(numBitsNextPowerOf2)))
            .build())
      case _ => None
    }
  }

  @sparkver("3.0 / 3.1 / 3.2")
  private def convertBloomFilterAgg(agg: AggregateFunction): Option[pb.PhysicalAggExprNode] = None

  @sparkver("3.3 / 3.4 / 3.5")
  private def convertBloomFilterMightContain(
      e: Expression,
      isPruningExpr: Boolean,
      fallback: Expression => pb.PhysicalExprNode): Option[pb.PhysicalExprNode] = {
    import org.apache.spark.sql.catalyst.expressions.BloomFilterMightContain
    e match {
      case e: BloomFilterMightContain =>
        val uuid = UUID.randomUUID().toString
        Some(NativeConverters.buildExprNode {
          _.setBloomFilterMightContainExpr(
            pb.BloomFilterMightContainExprNode
              .newBuilder()
              .setUuid(uuid)
              .setBloomFilterExpr(NativeConverters
                .convertExprWithFallback(e.bloomFilterExpression, isPruningExpr, fallback))
              .setValueExpr(NativeConverters
                .convertExprWithFallback(e.valueExpression, isPruningExpr, fallback)))
        })
      case _ => None
    }
  }

  @sparkver("3.0 / 3.1 / 3.2")
  private def convertBloomFilterMightContain(
      e: Expression,
      isPruningExpr: Boolean,
      fallback: Expression => pb.PhysicalExprNode): Option[pb.PhysicalExprNode] = None

  @sparkver("3.0")
  override def getAdaptiveInputPlan(exec: AdaptiveSparkPlanExec): SparkPlan = {
    exec.initialPlan
  }

  @sparkver("3.1 / 3.2 / 3.3 / 3.4 / 3.5")
  override def getAdaptiveInputPlan(exec: AdaptiveSparkPlanExec): SparkPlan = {
    exec.inputPlan
  }
}

case class ForceNativeExecutionWrapper(override val child: SparkPlan)
    extends ForceNativeExecutionWrapperBase(child) {

  @sparkver("3.2 / 3.3 / 3.4 / 3.5")
  override def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  @sparkver("3.0 / 3.1")
  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}

case class NativeExprWrapper(
    nativeExpr: pb.PhysicalExprNode,
    override val dataType: DataType,
    override val nullable: Boolean)
    extends NativeExprWrapperBase(nativeExpr, dataType, nullable) {

  @sparkver("3.2 / 3.3 / 3.4 / 3.5")
  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy()
}
