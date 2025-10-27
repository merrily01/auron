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

import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Generator
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.auron.plan._
import org.apache.spark.sql.execution.auron.plan.NativeBroadcastJoinBase
import org.apache.spark.sql.execution.auron.plan.NativeSortMergeJoinBase
import org.apache.spark.sql.execution.auron.shuffle.RssPartitionWriterBase
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.exchange.BroadcastExchangeLike
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.types.DataType
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.storage.FileSegment

import org.apache.auron.{protobuf => pb}
import org.apache.auron.jni.{AuronAdaptor, SparkAuronAdaptor}

abstract class Shims {

  def shimVersion: String

  def initExtension(): Unit = {}

  def onApplyingExtension(): Unit = {}

  def createConvertToNativeExec(child: SparkPlan): ConvertToNativeBase

  def createNativeAggExec(
      execMode: NativeAggBase.AggExecMode,
      requiredChildDistributionExpressions: Option[Seq[Expression]],
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute],
      initialInputBufferOffset: Int,
      child: SparkPlan): NativeAggBase

  def createNativeBroadcastExchangeExec(
      mode: BroadcastMode,
      child: SparkPlan): NativeBroadcastExchangeBase

  def createNativeBroadcastJoinExec(
      left: SparkPlan,
      right: SparkPlan,
      outputPartitioning: Partitioning,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      broadcastSide: BroadcastSide): NativeBroadcastJoinBase

  def createNativeSortMergeJoinExec(
      left: SparkPlan,
      right: SparkPlan,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      isSkewJoin: Boolean): NativeSortMergeJoinBase

  def createNativeShuffledHashJoinExec(
      left: SparkPlan,
      right: SparkPlan,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      isSkewJoin: Boolean): SparkPlan

  def createNativeExpandExec(
      projections: Seq[Seq[Expression]],
      output: Seq[Attribute],
      child: SparkPlan): NativeExpandBase

  def createNativeFilterExec(condition: Expression, child: SparkPlan): NativeFilterBase

  def createNativeGenerateExec(
      generator: Generator,
      requiredChildOutput: Seq[Attribute],
      outer: Boolean,
      generatorOutput: Seq[Attribute],
      child: SparkPlan): NativeGenerateBase

  def createNativeGlobalLimitExec(limit: Long, child: SparkPlan): NativeGlobalLimitBase

  def createNativeLocalLimitExec(limit: Long, child: SparkPlan): NativeLocalLimitBase

  def createNativeParquetInsertIntoHiveTableExec(
      cmd: InsertIntoHiveTable,
      child: SparkPlan): NativeParquetInsertIntoHiveTableBase

  def createNativeParquetScanExec(basedFileScan: FileSourceScanExec): NativeParquetScanBase

  def createNativeOrcScanExec(basedFileScan: FileSourceScanExec): NativeOrcScanBase

  def createNativeProjectExec(
      projectList: Seq[NamedExpression],
      child: SparkPlan): NativeProjectBase

  def createNativeRenameColumnsExec(
      child: SparkPlan,
      newColumnNames: Seq[String]): NativeRenameColumnsBase

  def createNativeShuffleExchangeExec(
      outputPartitioning: Partitioning,
      child: SparkPlan,
      shuffleOrigin: Option[Any] = None): NativeShuffleExchangeBase

  def createNativeSortExec(
      sortOrder: Seq[SortOrder],
      global: Boolean,
      child: SparkPlan): NativeSortBase

  def createNativeTakeOrderedExec(
      limit: Long,
      sortOrder: Seq[SortOrder],
      child: SparkPlan): NativeTakeOrderedBase

  def createNativePartialTakeOrderedExec(
      limit: Long,
      sortOrder: Seq[SortOrder],
      child: SparkPlan,
      metrics: Map[String, SQLMetric]): NativePartialTakeOrderedBase

  def createNativeUnionExec(children: Seq[SparkPlan], output: Seq[Attribute]): NativeUnionBase

  def createNativeWindowExec(
      windowExpression: Seq[NamedExpression],
      partitionSpec: Seq[Expression],
      orderSpec: Seq[SortOrder],
      groupLimit: Option[Int],
      outputWindowCols: Boolean,
      child: SparkPlan): NativeWindowBase

  def createNativeParquetSinkExec(
      sparkSession: SparkSession,
      table: CatalogTable,
      partition: Map[String, Option[String]],
      child: SparkPlan,
      metrics: Map[String, SQLMetric]): NativeParquetSinkBase

  def isNative(plan: SparkPlan): Boolean

  def getUnderlyingNativePlan(plan: SparkPlan): NativeSupports

  def getUnderlyingBroadcast(plan: SparkPlan): BroadcastExchangeLike

  def executeNative(plan: SparkPlan): NativeRDD

  def isQueryStageInput(plan: SparkPlan): Boolean

  def isShuffleQueryStageInput(plan: SparkPlan): Boolean

  def getChildStage(plan: SparkPlan): SparkPlan

  def simpleStringWithNodeId(plan: SparkPlan): String

  def setLogicalLink(exec: SparkPlan, basedExec: SparkPlan): SparkPlan

  def getRDDShuffleReadFull(rdd: RDD[_]): Boolean

  def setRDDShuffleReadFull(rdd: RDD[_], shuffleReadFull: Boolean): Unit

  // shim methods for expressions

  def convertMoreExprWithFallback(
      e: Expression,
      isPruningExpr: Boolean,
      fallback: Expression => pb.PhysicalExprNode): Option[pb.PhysicalExprNode]

  def convertMoreAggregateExpr(e: AggregateExpression): Option[pb.PhysicalExprNode]

  def getLikeEscapeChar(expr: Expression): Char

  def getAggregateExpressionFilter(expr: Expression): Option[Expression]

  def createFileSegment(file: File, offset: Long, length: Long, numRecords: Long): FileSegment

  def commit(
      dep: ShuffleDependency[_, _, _],
      shuffleBlockResolver: IndexShuffleBlockResolver,
      tempDataFile: File,
      mapId: Long,
      partitionLengths: Array[Long],
      dataSize: Long,
      context: TaskContext): MapStatus

  def getRssPartitionWriter(
      handle: ShuffleHandle,
      mapId: Int,
      metrics: ShuffleWriteMetricsReporter,
      numPartitions: Int): Option[RssPartitionWriterBase]

  def getMapStatus(
      shuffleServerId: BlockManagerId,
      partitionLengthMap: Array[Long],
      mapId: Long): MapStatus

  def getShuffleWriteExec(
      input: pb.PhysicalPlanNode,
      nativeOutputPartitioning: pb.PhysicalRepartition.Builder): pb.PhysicalPlanNode

  def convertMoreSparkPlan(exec: SparkPlan): Option[SparkPlan]

  def getSqlContext(sparkPlan: SparkPlan): SQLContext

  def createNativeExprWrapper(
      nativeExpr: pb.PhysicalExprNode,
      dataType: DataType,
      nullable: Boolean): Expression

  def getPartitionedFile(
      partitionValues: InternalRow,
      filePath: String,
      offset: Long,
      size: Long): PartitionedFile

  def getMinPartitionNum(sparkSession: SparkSession): Int

  def postTransform(plan: SparkPlan, sc: SparkContext): Unit = {}

  def getAdaptiveInputPlan(exec: AdaptiveSparkPlanExec): SparkPlan
}

object Shims {
  lazy val get: Shims = {
    AuronAdaptor.initInstance(new SparkAuronAdaptor)
    classOf[Shims].getClassLoader
      .loadClass("org.apache.spark.sql.auron.ShimsImpl")
      .newInstance()
      .asInstanceOf[Shims]
  }
}
