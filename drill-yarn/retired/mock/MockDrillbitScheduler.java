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
package org.apache.drill.yarn.mock;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.drill.yarn.appMaster.AbstractTaskManager;
import org.apache.drill.yarn.appMaster.PersistentTaskScheduler;
import org.apache.drill.yarn.appMaster.Task;
import org.apache.drill.yarn.appMaster.TaskSpec;
import org.apache.drill.yarn.core.ContainerRequestSpec;
import org.apache.drill.yarn.core.LaunchSpec;

public class MockDrillbitScheduler extends PersistentTaskScheduler
{

  public static class MockDrilbitManager extends AbstractTaskManager
  {
    @Override
    public boolean stop(Task task) {
      File tmp = new File("/tmp");
      String stopFileName = task.getContainerId().toString() + ".kill";
      File stopFile = new File(tmp, stopFileName);
      PrintWriter writer;
      try {
        writer = new PrintWriter(new FileWriter(stopFile));
      } catch (IOException e) {
        return false;
      }
      writer.println("bye");
      writer.close();
      return true;
    }
  }

  public MockDrillbitScheduler(int quantity) {
    super("mock", "mockDrillbit", quantity);

    setTaskManager(new MockDrilbitManager());

    ContainerRequestSpec containerSpec = new ContainerRequestSpec();
    // containerSpec.priority = 1;
    containerSpec.memoryMb = 128;
    containerSpec.vCores = 1;

    LaunchSpec workerSpec = new LaunchSpec();
    workerSpec.mainClass = MockDrillBit.class.getName();
    workerSpec.classPath.add("/Users/progers/git/drill-yarn/target/drill-yarn-1.6-SNAPSHOT.jar");

    taskSpec = new TaskSpec();
    taskSpec.containerSpec = containerSpec;
    taskSpec.launchSpec = workerSpec;
    taskSpec.maxRetries = 10;
  }
}
