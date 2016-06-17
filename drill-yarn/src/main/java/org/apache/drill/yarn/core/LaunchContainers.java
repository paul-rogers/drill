/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.yarn.core;

import java.util.List;

import org.apache.drill.yarn.appMaster.TaskState;

/**
 * Worker thread to perform the task of starting a container outside
 * of the event callback thread.
 *
 * Not currently used.
 */

public class LaunchContainers implements Runnable {
  private final List<TaskState> toLaunch;

  public LaunchContainers(List<TaskState> toLaunch) {
    this.toLaunch = toLaunch;
  }

  @Override
  public void run() {
//    for (TaskState cState : toLaunch) {
//      Container container = cState.getContainer();
//      if (cState.isDoomed()) {
//        controller.launchCancelled(cState);
//        yarn.releaseContainer(container);
//        continue;
//      }
//      LOG.trace("Launching container " + container.getId());
//      ContainerLaunchContext context;
//      try {
//        context = yarn.createLaunchContext(cState.prepareTaskSpec());
//        controller.taskLaunching(cState);
//        yarn.startContainerAsync(container, context);
//      } catch (Exception e) {
//        LOG.error("Create of task launch context failed", e);
//        controller.launchFailed(cState, e);
//      }
//    }
  }

}