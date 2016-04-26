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
package org.apache.drill.yarn.appMaster;

public class DrillbitScheduler extends AbstractDrillbitScheduler
{
  public DrillbitScheduler(TaskSpec taskSpec, int quantity) {
    super("Basic-Drillbit", quantity);
    this.taskSpec = taskSpec;

//    ContainerRequestSpec containerSpec = new ContainerRequestSpec();
//    // containerSpec.priority = 1;
//    containerSpec.memoryMb = 1025;
//    containerSpec.vCores = 4;
//
//    LaunchSpec workerSpec = new LaunchSpec();
//    workerSpec.env.put( "DRILL_HOME", "/Users/progers/play/apache-drill-1.5.0" );
//    workerSpec.command = "$DRILL_HOME/bin/drillbit.sh";
//    workerSpec.cmdArgs.add("yarn_start");
//
//    taskSpec = new TaskSpec();
//    taskSpec.containerSpec = containerSpec;
//    taskSpec.launchSpec = workerSpec;
//    taskSpec.maxRetries = 10;
  }
}
