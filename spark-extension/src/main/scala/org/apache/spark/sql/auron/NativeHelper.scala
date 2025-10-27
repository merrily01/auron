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

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer

import org.apache.arrow.vector.types.pojo.Schema
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.Partition
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.auron.arrowio.util.ArrowUtils
import org.apache.spark.sql.execution.auron.arrowio.util.ArrowUtils.ROOT_ALLOCATOR
import org.apache.spark.sql.execution.auron.columnar.ColumnarHelper
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.CompletionIterator

import org.apache.auron.metric.SparkMetricNode
import org.apache.auron.protobuf.PhysicalPlanNode

object NativeHelper extends Logging {
  val currentUser: UserGroupInformation = UserGroupInformation.getCurrentUser
  private val conf: SparkConf = SparkEnv.get.conf

  val totalMemory: Long = {
    val MEMORY_OVERHEAD_FACTOR = 0.10
    val MEMORY_OVERHEAD_MIN = 384L
    if (TaskContext.get() != null) {
      // executor side
      val executorMemoryMiB = conf.get(config.EXECUTOR_MEMORY)
      val executorMemoryOverheadMiB = conf
        .get(config.EXECUTOR_MEMORY_OVERHEAD)
        .getOrElse(
          math.max((MEMORY_OVERHEAD_FACTOR * executorMemoryMiB).toLong, MEMORY_OVERHEAD_MIN))
      (executorMemoryMiB + executorMemoryOverheadMiB) * 1024L * 1024L
    } else {
      // driver side
      val driverMemoryMiB = conf.get(config.DRIVER_MEMORY)
      val driverMemoryOverheadMiB = conf
        .get(config.DRIVER_MEMORY_OVERHEAD)
        .getOrElse(
          math.max((MEMORY_OVERHEAD_FACTOR * driverMemoryMiB).toLong, MEMORY_OVERHEAD_MIN))
      (driverMemoryMiB + driverMemoryOverheadMiB) * 1024L * 1024L
    }
  }

  val nativeMemory: Long = {
    val heapMemory = Runtime.getRuntime.maxMemory()
    val offheapMemory = totalMemory - heapMemory
    logWarning(s"memory total: $totalMemory, onheap: $heapMemory, offheap: $offheapMemory")
    offheapMemory
  }

  def isNative(exec: SparkPlan): Boolean =
    Shims.get.isNative(exec)

  def getUnderlyingNativePlan(exec: SparkPlan): NativeSupports =
    Shims.get.getUnderlyingNativePlan(exec)

  def executeNative(exec: SparkPlan): NativeRDD = {
    Shims.get.executeNative(exec)
  }

  def executeNativePlan(
      nativePlan: PhysicalPlanNode,
      metrics: SparkMetricNode,
      partition: Partition,
      context: Option[TaskContext]): Iterator[InternalRow] = {

    if (partition.index == 0 && metrics != null && context.nonEmpty) {
      metrics.foreach(_.add("stage_id", context.get.stageId()))
    }
    if (nativePlan == null) {
      return Iterator.empty
    }
    var auronCallNativeWrapper = new org.apache.auron.jni.AuronCallNativeWrapper(
      ROOT_ALLOCATOR,
      nativePlan,
      metrics,
      partition.index,
      context.map(_.stageId()).getOrElse(0),
      context.map(_.taskAttemptId().toInt).getOrElse(0),
      NativeHelper.nativeMemory)

    context.foreach(
      _.addTaskCompletionListener[Unit]((_: TaskContext) => auronCallNativeWrapper.close()))
    context.foreach(_.addTaskFailureListener((_, _) => auronCallNativeWrapper.close()))

    val rowIterator = new Iterator[InternalRow] {
      private var arrowSchema: Schema = _
      private var schema: StructType = _
      private var toUnsafe: UnsafeProjection = _
      private val batchRows: ArrayBuffer[InternalRow] = ArrayBuffer()
      private var batchCurRowIdx = 0

      override def hasNext: Boolean = {
        // if current batch is not empty, return true
        if (batchCurRowIdx < batchRows.length) {
          return true
        }

        // clear current batch
        batchRows.clear()
        batchCurRowIdx = 0

        if (auronCallNativeWrapper.loadNextBatch(root => {
            if (arrowSchema == null) {
              arrowSchema = auronCallNativeWrapper.getArrowSchema
              schema = ArrowUtils.fromArrowSchema(arrowSchema)
              toUnsafe = UnsafeProjection.create(schema)
            }
            batchRows.append(
              ColumnarHelper
                .rootRowsIter(root)
                .map(row => toUnsafe(row).copy().asInstanceOf[InternalRow])
                .toSeq: _*)
          })) {
          return hasNext
        }
        // clear current batch
        arrowSchema = null
        batchRows.clear()
        batchCurRowIdx = 0
        false
      }

      override def next(): InternalRow = {
        val batchRow = batchRows(batchCurRowIdx)
        batchCurRowIdx += 1
        batchRow
      }
    }

    CompletionIterator[InternalRow, Iterator[InternalRow]](
      rowIterator,
      () -> {
        synchronized {
          auronCallNativeWrapper.close()
        }
      })
  }

  def getDefaultNativeMetrics(sc: SparkContext): Map[String, SQLMetric] = {
    def metric(name: String) = SQLMetrics.createMetric(sc, name)
    def nanoTimingMetric(name: String) = SQLMetrics.createNanoTimingMetric(sc, name)
    def sizeMetric(name: String) = SQLMetrics.createSizeMetric(sc, name)

    var metrics = TreeMap(
      "stage_id" -> metric("stageId"),
      "output_rows" -> metric("Native.output_rows"),
      "output_batches" -> metric("Native.output_batches"),
      "elapsed_compute" -> nanoTimingMetric("Native.elapsed_compute"),
      "build_hash_map_time" -> nanoTimingMetric("Native.build_hash_map_time"),
      "probed_side_hash_time" -> nanoTimingMetric("Native.probed_side_hash_time"),
      "probed_side_search_time" -> nanoTimingMetric("Native.probed_side_search_time"),
      "probed_side_compare_time" -> nanoTimingMetric("Native.probed_side_compare_time"),
      "build_output_time" -> nanoTimingMetric("Native.build_output_time"),
      "fallback_sort_merge_join_time" -> nanoTimingMetric("Native.fallback_sort_merge_join_time"),
      "mem_spill_count" -> metric("Native.mem_spill_count"),
      "mem_spill_size" -> sizeMetric("Native.mem_spill_size"),
      "mem_spill_iotime" -> nanoTimingMetric("Native.mem_spill_iotime"),
      "disk_spill_size" -> sizeMetric("Native.disk_spill_size"),
      "disk_spill_iotime" -> nanoTimingMetric("Native.disk_spill_iotime"),
      "shuffle_write_total_time" -> nanoTimingMetric("Native.shuffle_write_total_time"),
      "shuffle_read_total_time" -> nanoTimingMetric("Native.shuffle_read_total_time"))

    if (AuronConf.INPUT_BATCH_STATISTICS_ENABLE.booleanConf()) {
      metrics ++= TreeMap(
        "input_batch_count" -> metric("Native.input_batches"),
        "input_row_count" -> metric("Native.input_rows"),
        "input_batch_mem_size" -> sizeMetric("Native.input_mem_bytes"))
    }
    metrics
  }

  private def getDefaultNativeFileMetrics(sc: SparkContext): Map[String, SQLMetric] = {
    TreeMap(
      "bytes_scanned" -> SQLMetrics.createSizeMetric(sc, "Native.bytes_scanned"),
      "io_time" -> SQLMetrics.createNanoTimingMetric(sc, "Native.io_time"),
      "io_time_getfs" -> SQLMetrics.createNanoTimingMetric(sc, "Native.io_time_getfs"),
      // Parquet metrics
      "predicate_evaluation_errors" -> SQLMetrics.createMetric(
        sc,
        "Native.predicate_evaluation_errors"),
      "row_groups_matched_bloom_filter" -> SQLMetrics.createMetric(
        sc,
        "Native.row_groups_matched_bloom_filter"),
      "row_groups_pruned_bloom_filter" -> SQLMetrics.createMetric(
        sc,
        "Native.row_groups_pruned_bloom_filter"),
      "row_groups_matched_statistics" -> SQLMetrics.createMetric(
        sc,
        "Native.row_groups_matched_statistics"),
      "row_groups_pruned_statistics" -> SQLMetrics.createMetric(
        sc,
        "Native.row_groups_pruned_statistics"),
      "pushdown_rows_filtered" -> SQLMetrics.createMetric(sc, "Native.pushdown_rows_filtered"),
      "pushdown_eval_time" -> SQLMetrics.createNanoTimingMetric(sc, "Native.pushdown_eval_time"),
      "page_index_rows_filtered" -> SQLMetrics.createMetric(
        sc,
        "Native.page_index_rows_filtered"),
      "page_index_eval_time" -> SQLMetrics.createNanoTimingMetric(
        sc,
        "Native.page_index_eval_time"))
  }

  def getNativeFileScanMetrics(sc: SparkContext): Map[String, SQLMetric] = TreeMap(
    getDefaultNativeMetrics(sc)
      .filterKeys(Set("stage_id", "output_rows", "elapsed_compute"))
      .toSeq ++ getDefaultNativeFileMetrics(sc).toSeq: _*)
}
