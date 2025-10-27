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
package org.apache.spark.sql.execution.auron.shuffle

import java.util.UUID

import org.apache.spark.{Partition, ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{ShuffleHandle, ShuffleWriteMetricsReporter}
import org.apache.spark.shuffle.ShuffleWriter
import org.apache.spark.sql.auron.{NativeHelper, NativeRDD, Shims}

import org.apache.auron.jni.JniBridge
import org.apache.auron.protobuf.{PhysicalPlanNode, RssShuffleWriterExecNode}
import org.apache.auron.sparkver

abstract class AuronRssShuffleWriterBase[K, V](metrics: ShuffleWriteMetricsReporter)
    extends ShuffleWriter[K, V] {

  private var rpw: RssPartitionWriterBase = _
  private var mapId: Int = 0

  def getRssPartitionWriter(
      handle: ShuffleHandle,
      mapId: Int,
      metrics: ShuffleWriteMetricsReporter,
      numPartitions: Int): RssPartitionWriterBase

  def nativeRssShuffleWrite(
      nativeShuffleRDD: NativeRDD,
      dep: ShuffleDependency[_, _, _],
      mapId: Int,
      context: TaskContext,
      partition: Partition,
      numPartitions: Int): Unit = {

    this.mapId = mapId
    this.rpw = getRssPartitionWriter(dep.shuffleHandle, mapId, metrics, numPartitions)
    var success = false

    try {
      val jniResourceId = s"RssPartitionWriter:${UUID.randomUUID().toString}"
      JniBridge.putResource(jniResourceId, rpw)
      val nativeRssShuffleWriterExec = PhysicalPlanNode
        .newBuilder()
        .setRssShuffleWriter(
          RssShuffleWriterExecNode
            .newBuilder(nativeShuffleRDD.nativePlan(partition, context).getRssShuffleWriter)
            .setRssPartitionWriterResourceId(jniResourceId)
            .build())
        .build()

      val iterator = NativeHelper.executeNativePlan(
        nativeRssShuffleWriterExec,
        nativeShuffleRDD.metrics,
        partition,
        Some(context))
      success = iterator.toArray.isEmpty

    } finally {
      rpw.close(success)
    }
  }

  def rssStop(success: Boolean): Option[MapStatus]

  @sparkver("3.2 / 3.3 / 3.4 / 3.5")
  override def getPartitionLengths(): Array[Long] = rpw.getPartitionLengthMap

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    throw new UnsupportedOperationException("should use nativeRssShuffleWrite")
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    if (!success) {
      return None
    }

    val mapStatus: Option[MapStatus] = rssStop(success)
    mapStatus match {
      case Some(status) =>
        Some(status)
      case None =>
        // Fall back to get mapStatus manually
        val blockManagerId = SparkEnv.get.blockManager.shuffleServerId
        Some(Shims.get.getMapStatus(blockManagerId, rpw.getPartitionLengthMap, mapId))
    }
  }
}
