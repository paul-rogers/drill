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

public class TestCommand extends ClientCommand {

  public TestCommand(CommandLineOptions opts) {
  }

  @Override
  public void run() {
    AppSpec master = new AppSpec();
    master.vmArgs = "-Xmx256M";
    master.mainClass = "com.hortonworks.simpleyarnapp.ApplicationMaster";
    master.cmdArgs.add("/bin/date");
    master.cmdArgs.add("2");
    master.memoryMb = 256;
    master.vCores = 1;
    master.appName = "simpleapp2";
//        master.resources.put( "simpleapp.jar", "/apps/simple/simple-yarn-app-1.1.0.jar" );
    master.resources.put("simpleapp.jar", "hdfs:///apps/simple/simple-yarn-app-1.1.0.jar");
//        AppResource jar = new AppResource( );
//        jar.key = "simpleapp.jar";
//        jar.dfsPath = "hdfs:///apps/simple/simple-yarn-app-1.1.0.jar";
//        jar.localPath = new File( "/Users/progers/git/simple-yarn-app/target/simple-yarn-app-1.1.0.jar" );
////        master.localizedResources.put( "simpleapp.jar", "/apps/simple/simple-yarn-app-1.1.0.jar" );
////        master.jarPath = "/Users/progers/git/simple-yarn-app/target/simple-yarn-app-1.1.0.jar";
////        master.jar = "simpleapp.jar";
//        master.resources.add( jar );

    YarnRMClient client = new YarnRMClient();
    try {
      client.launchAppMaster(master);
      client.waitForCompletion();
    } catch (YarnClientException e) {
      e.printStackTrace();
    }
  }

}
