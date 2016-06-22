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

package org.apache.spark.scheduler

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.TaskLocality._

import scala.collection.mutable.{ArrayBuffer, HashSet}
import scala.util.Random

/**
 * Created by tiantian on 17/06/16.
 */
class AdvancedTaskSchedulerImpl(sc: SparkContext,
                                 maxTaskFailures: Int,
                                 isLocal: Boolean = false)
  extends TaskSchedulerImpl (sc, maxTaskFailures, isLocal){

  def this(sc: SparkContext) = this(sc, sc.conf.getInt("spark.task.maxFailures", 4))

  /**
   * check whether the input of a task is local to a given host
   * @param host
   * @param task
   * @return
   */
  def isLocal(host:String, task:Task[_]):Boolean = {
   if(task.preferredLocations == null || task.preferredLocations.isEmpty) {
     false
   } else {
     val preferredHosts = task.preferredLocations.map(_.host)
     preferredHosts.contains(host)
   }
  }

  /**
   * offer resources to a single taskset which might have many tasks
   * @param taskSet
   * @param maxLocality
   * @param shuffledOffers
   * @param availableCpus
   * @param tasks
   * @return
   */
  private def resourceOfferSingleTaskSet(taskSet: TaskSetManager,
                                          maxLocality: TaskLocality,
                                          shuffledOffers: Seq[WorkerOffer],
                                          availableCpus: Array[Int],
                                          tasks: Seq[ArrayBuffer[TaskDescription]]) : Boolean = {
    var launchedTask = false
    val slotsIdx = ArrayBuffer.empty[Int]
    val slots = (for (idx <- 0 until shuffledOffers.size) yield {
      val w = shuffledOffers(idx)
      val cores = availableCpus(idx)
      for( i <- 0 until cores) slotsIdx += idx
      Seq.fill(cores){ w.copy(cores = 1)}
    }).flatten
    logDebug(s"slotsIdx: ${slotsIdx.mkString(",")}")
    val workerSize = slots.length
    val pendingTasks:Array[Task[_]] = taskSet.getAllPendingTasks
    val jobSize = pendingTasks.length
    logDebug(s"offer size: ${shuffledOffers.size}")
    logDebug(s"worker size: ${workerSize}")
    logDebug(s"job size: ${jobSize}")
    if(workerSize > 0 && jobSize > 0){
      val schedStart = System.currentTimeMillis()
      val costMatrix = Array.fill(workerSize) {
        Array.fill(jobSize) {
          0D
        }
      }
      //update costMatrix according to the distance for each
      for {
        i <- 0 until workerSize
        j <- 0 until jobSize
      } {
        val cost = if (isLocal(slots(i).host, pendingTasks(j))) 1D else 5D
        costMatrix(i)(j) = cost
      }
      //find out assignment for the costMatrix
      val algo = new HungarianAlgorithm(costMatrix)
      val res = algo.execute()
      logInfo(s"matched results: ${res.mkString(",")}")
      val avalRes = res.filter(_ >= 0)
      val schedEnd = System.currentTimeMillis()
      logInfo(s"job matching finished in ${schedEnd - schedStart} ms.")
      if(avalRes.length > 0){
        val assigns = taskSet.resourceOffers(avalRes, slots, maxLocality)
        logInfo(s"assigned offers: ${assigns.mkString(",")}")
        if(assigns.length > 0) launchedTask = true
        for (assign <- assigns){
          val idx = slotsIdx(assign._1)
          val execId = shuffledOffers(idx).executorId
          val host = shuffledOffers(idx).host
          tasks(idx) += assign._2
          val tid = assign._2.taskId
          taskIdToTaskSetId(tid) = taskSet.taskSet.id
          taskIdToExecutorId(tid) = execId
          executorsByHost(host) += execId
          availableCpus(idx) -= 1
          logDebug(s"assigned task to executor: ${idx}..")
        }
      }
    }
    launchedTask
  }

  /**
   * Called by cluster manager to offer resources on slaves. We respond by asking our active task
   * sets for tasks in order of priority. We fill each node with tasks in a round-robin manner so
   * that tasks are balanced across the cluster.
   */
  override  def resourceOffers(offers: Seq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {
    // Mark each slave as alive and remember its hostname
    // Also track if new executor is added
    var newExecAvail = false
    for (o <- offers) {
      executorIdToHost(o.executorId) = o.host
      activeExecutorIds += o.executorId
      if (!executorsByHost.contains(o.host)) {
        executorsByHost(o.host) = new HashSet[String]()
        executorAdded(o.executorId, o.host)
        newExecAvail = true
      }
      for (rack <- getRackForHost(o.host)) {
        hostsByRack.getOrElseUpdate(rack, new HashSet[String]()) += o.host
      }
    }

    // Randomly shuffle offers to avoid always placing tasks on the same set of workers.
    val shuffledOffers = Random.shuffle(offers)
    // Build a list of tasks to assign to each worker.
    val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores))
    val availableCpus = shuffledOffers.map(o => o.cores).toArray
    val sortedTaskSets = rootPool.getSortedTaskSetQueue
    for (taskSet <- sortedTaskSets) {
      logDebug("parentName: %s, name: %s, runningTasks: %s".format(
        taskSet.parent.name, taskSet.name, taskSet.runningTasks))
      if (newExecAvail) {
        taskSet.executorAdded()
      }
    }

    // Take each TaskSet in our scheduling order, and then offer it each node in increasing order
    // of locality levels so that it gets a chance to launch local tasks on all of them.
    // NOTE: the preferredLocality order: PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
    var launchedTask = false
    for (taskSet <- sortedTaskSets; maxLocality <- taskSet.myLocalityLevels) {
      do {
        launchedTask = resourceOfferSingleTaskSet(
          taskSet, maxLocality, shuffledOffers, availableCpus, tasks)
      } while (launchedTask)
    }

    if (tasks.size > 0) {
      hasLaunchedTask = true
    }
    return tasks
  }

}
