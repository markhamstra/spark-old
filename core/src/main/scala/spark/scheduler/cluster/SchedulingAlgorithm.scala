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

package spark.scheduler.cluster

/**
 * An interface for sort algorithm
 * FIFO: FIFO algorithm between TaskSetManagers
 * FS: FS algorithm between Pools, and FIFO or FS within Pools
 */
private[spark] trait SchedulingAlgorithm {
  def comparator(s1: Schedulable, s2: Schedulable): Boolean
}

private[spark] class FIFOSchedulingAlgorithm extends SchedulingAlgorithm {
  override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
    val schedSignum = math.signum(s1.priority - s2.priority)
    if (schedSignum != 0) schedSignum < 0
    else math.signum(s1.stageId - s2.stageId) < 0  // stageIds must be distinct
  }
}

private[spark] class FairSchedulingAlgorithm extends SchedulingAlgorithm {
  override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
    if (s1.isNeedy == s2.isNeedy) {
      val compare = if (s1.isNeedy) s1.minShareRatio.compareTo(s2.minShareRatio)
                    else s1.taskToWeightRatio.compareTo(s2.taskToWeightRatio)
      if (compare == 0) s1.name < s2.name
      else compare < 0
    } else s1.isNeedy && !s2.isNeedy
  }
}

