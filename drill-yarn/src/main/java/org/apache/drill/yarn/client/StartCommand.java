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
package org.apache.drill.yarn.client;

import org.apache.drill.yarn.core.AppSpec;
import org.apache.drill.yarn.core.YarnClientException;
import org.apache.drill.yarn.core.YarnRMClient;

public class StartCommand extends ClientCommand {

  public StartCommand(CommandLineOptions opts) {
  }

  @Override
  public void run() {

    // Clunky initial version.
    // TODO: get main class name from class itself
    // TODO: Upload jar file to HDFS
    // TODO: Get launch config from a config file
    // TODO: change cmd args to something specific to Drill
    // TODO: Change app name to drill + cluster name

    AppSpec master = new AppSpec();
    master.vmArgs = "-Xmx256M";
    master.mainClass = "org.apache.drill.yarn.appMaster.ApplicationMaster";
    master.cmdArgs.add("/bin/date");
    master.cmdArgs.add("2");
    master.memoryMb = 256;
    master.vCores = 1;
    master.appName = "drill-test";
    master.resources.put("drillTest.jar", "hdfs:///apps/simple/drill-yarn-1.6-SNAPSHOT.jar");

    YarnRMClient client = new YarnRMClient();
    try {
      client.launchAppMaster(master);
      client.waitForCompletion();
    } catch (YarnClientException e) {
      e.printStackTrace();
    }
  }

}
