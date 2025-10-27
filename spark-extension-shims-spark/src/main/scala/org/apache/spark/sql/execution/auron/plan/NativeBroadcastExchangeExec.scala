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

import org.apache.spark.broadcast
import org.apache.spark.sql.auron.NativeSupports
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.SparkPlan

import org.apache.auron.sparkver

case class NativeBroadcastExchangeExec(mode: BroadcastMode, override val child: SparkPlan)
    extends NativeBroadcastExchangeBase(mode, child)
    with NativeSupports {

  override val getRunId: UUID = UUID.randomUUID()

  override def runtimeStatistics: Statistics = {
    val dataSize = metrics("dataSize").value
    val rowCount = metrics("numOutputRows").value
    Statistics(dataSize, Some(rowCount))
  }

  @transient
  override lazy val completionFuture: scala.concurrent.Future[broadcast.Broadcast[Any]] = {
    relationFuturePromise.future
  }

  @sparkver("3.2 / 3.3 / 3.4 / 3.5")
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  @sparkver("3.0 / 3.1")
  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}
