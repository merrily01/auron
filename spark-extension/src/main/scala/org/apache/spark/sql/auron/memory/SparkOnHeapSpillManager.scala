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
package org.apache.spark.sql.auron.memory

import java.nio.ByteBuffer

import scala.collection.concurrent
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryConsumer
import org.apache.spark.memory.MemoryMode
import org.apache.spark.memory.auron.OnHeapSpillManagerHelper
import org.apache.spark.sql.auron.AuronConf
import org.apache.spark.storage.BlockManager
import org.apache.spark.util.Utils

import org.apache.auron.memory.OnHeapSpillManager

class SparkOnHeapSpillManager(taskContext: TaskContext)
    extends MemoryConsumer(
      taskContext.taskMemoryManager,
      taskContext.taskMemoryManager.pageSizeBytes(),
      MemoryMode.ON_HEAP)
    with OnHeapSpillManager
    with Logging {
  import org.apache.spark.sql.auron.memory.SparkOnHeapSpillManager._

  private val _blockManager = SparkEnv.get.blockManager
  private val spills = ArrayBuffer[Option[OnHeapSpill]]()
  private var numHoldingSpills = 0

  // release all spills on task completion
  taskContext.addTaskCompletionListener { _ =>
    synchronized {
      logInfo(s"task completed, start releasing all holding spills (count=$numHoldingSpills)")
      spills.flatten.foreach(spill => releaseSpill(spill.id))
      all.remove(taskContext.taskAttemptId())
    }
  }

  def blockManager: BlockManager = _blockManager

  def memUsed: Long = getUsed

  /**
   * check whether we should spill to onheap memory
   * @return
   */
  @SuppressWarnings(Array("unused"))
  override def isOnHeapAvailable: Boolean = {
    // if driver, tc always null.
    if (taskContext == null) {
      return false
    }
    val memoryPool = OnHeapSpillManagerHelper.getOnHeapExecutionMemoryPool
    val memoryUsed = memoryPool.memoryUsed
    val memoryFree = memoryPool.memoryFree
    val memoryUsedRatio = (memoryUsed + 1.0) / (memoryUsed + memoryFree + 1.0)
    val jvmMemoryFree = Runtime.getRuntime.freeMemory()
    val jvmMemoryUsed = Runtime.getRuntime.totalMemory() - jvmMemoryFree
    val jvmMemoryUsedRatio = (jvmMemoryUsed + 1.0) / (jvmMemoryUsed + jvmMemoryFree + 1.0)

    logInfo(
      s"current on-heap execution memory usage:" +
        s" used=${Utils.bytesToString(memoryUsed)}," +
        s" free=${Utils.bytesToString(memoryFree)}," +
        s" ratio=$memoryUsedRatio")
    logInfo(
      s"current jvm memory usage:" +
        s" jvm total used: ${Utils.bytesToString(jvmMemoryUsed)}," +
        s" jvm total free: ${Utils.bytesToString(jvmMemoryFree)}," +
        s" ratio=$jvmMemoryUsedRatio")

    // we should have at least 10% free memory
    val maxRatio = AuronConf.ON_HEAP_SPILL_MEM_FRACTION.doubleConf()
    memoryUsedRatio < maxRatio && jvmMemoryUsedRatio < maxRatio
  }

  /**
   * allocate a new spill and return its id
   * @return
   *   allocated spill id
   */
  override def newSpill(): Int = {
    synchronized {
      val spill = OnHeapSpill(this, spills.length)
      spills.append(Some(spill))
      numHoldingSpills += 1

      logInfo(s"allocated a spill, task=${taskContext.taskAttemptId}, id=${spill.id}")
      dumpStatus()
      spill.id
    }
  }

  override def writeSpill(spillId: Int, data: ByteBuffer): Unit = {
    spills(spillId)
      .getOrElse(
        throw new RuntimeException(
          s"writing released spill task=${taskContext.taskAttemptId}, id=${spillId}"))
      .write(data)
  }

  override def readSpill(spillId: Int, buf: ByteBuffer): Int = {
    spills(spillId)
      .getOrElse(
        throw new RuntimeException(
          s"reading released spill, task=${taskContext.taskAttemptId}, id=${spillId}"))
      .read(buf)
  }

  def getSpillSize(spillId: Int): Long = {
    spills(spillId).map(_.size).getOrElse(0)
  }

  override def getSpillDiskUsage(spillId: Int): Long = {
    spills(spillId).map(_.diskUsed).getOrElse(0)
  }

  override def getSpillDiskIOTime(spillId: Int): Long = {
    spills(spillId).map(_.diskIOTime).getOrElse(0) // time unit: ns
  }

  override def releaseSpill(spillId: Int): Unit = {
    spills(spillId) match {
      case Some(spill) =>
        spill.release()
        numHoldingSpills -= 1
        logInfo(s"released a spill, task=${taskContext.taskAttemptId}, id=$spillId")
        dumpStatus()
      case None =>
    }
    spills(spillId) = None
  }

  override def spill(size: Long, trigger: MemoryConsumer): Long = {
    if (trigger != this && memUsed * 2 < this.taskMemoryManager.getMemoryConsumptionForThisTask) {
      return 0L
    }

    logInfo(s"starts spilling to disk, size=${Utils.bytesToString(size)}}")
    dumpStatus()
    var totalFreed = 0L

    // prefer the max spill, to avoid generating too many small files
    Utils.tryWithSafeFinally {
      synchronized {
        val sortedSpills = spills.seq.sortBy(0 - _.map(_.memUsed).getOrElse(0L))
        sortedSpills.foreach {
          case Some(spill) if spill.memUsed > 0 =>
            totalFreed += spill.spill()
            if (totalFreed >= size) {
              return totalFreed
            }
          case _ =>
        }
      }
    } {
      logInfo(s"finished spilling to disk, freed=${Utils.bytesToString(totalFreed)}")
      dumpStatus()
    }
    totalFreed
  }

  private def dumpStatus(): Unit = {
    logInfo(
      "status" +
        s": numHoldingSpills=$numHoldingSpills" +
        s", memUsed=${Utils.bytesToString(memUsed)}")
  }
}

object SparkOnHeapSpillManager extends Logging {
  val all: mutable.Map[Long, OnHeapSpillManager] = concurrent.TrieMap[Long, OnHeapSpillManager]()

  def current: OnHeapSpillManager = {
    val tc = TaskContext.get
    all.getOrElseUpdate(tc.taskAttemptId(), new SparkOnHeapSpillManager(tc))
  }
}
