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

import org.apache.drill.yarn.appMaster.AMWrapperException;
import org.apache.drill.yarn.appMaster.AMYarnFacadeImpl;
import org.apache.drill.yarn.appMaster.ControllerFactory;
import org.apache.drill.yarn.appMaster.Dispatcher;
import org.apache.drill.yarn.appMaster.DrillbitScheduler;
import org.apache.drill.yarn.appMaster.Scheduler;
import org.apache.drill.yarn.appMaster.TaskSpec;
import org.apache.drill.yarn.appMaster.YarnFacadeException;
import org.apache.drill.yarn.appMaster.ControllerFactory.ControllerFactoryException;
import org.apache.drill.yarn.core.ContainerRequestSpec;
import org.apache.drill.yarn.core.LaunchSpec;
import org.apache.drill.yarn.zk.ZKClusterCoordinatorDriver;
import org.apache.drill.yarn.zk.ZKConfigException;
import org.apache.drill.yarn.zk.ZKRegistry;

public class TestDrillbitFactory implements ControllerFactory
{
  private int count = 1;
  private String zkConnect = "localhost:2181/drill/drillbits1";
  private int pollPeriodMs = 1000;
  private int timerPeriodMs = 2000;

  public TestDrillbitFactory setCount(int count) {
    this.count = count;
    return this;
  }

  public TestDrillbitFactory setPollPeriodMs(int ms) {
    pollPeriodMs = ms;
    return this;
  }

  public TestDrillbitFactory setTimerPeriodMs(int ms) {
    timerPeriodMs = ms;
    return this;
  }

  public TestDrillbitFactory setZKConnect(String connect) {
    zkConnect = connect;
    return this;
  }

  @Override
  public Dispatcher build() throws ControllerFactoryException {
    Dispatcher dispatcher = new Dispatcher(timerPeriodMs);
    AMYarnFacadeImpl yarn = new AMYarnFacadeImpl(pollPeriodMs);
    try {
      dispatcher.setYarn(yarn);
    } catch (YarnFacadeException e) {
      throw new ControllerFactoryException( "Yarn Setup failed", e );
    }

    ContainerRequestSpec containerSpec = new ContainerRequestSpec();
    // containerSpec.priority = 1;
    containerSpec.memoryMb = 1025;
    containerSpec.vCores = 4;

    LaunchSpec workerSpec = new LaunchSpec();
    workerSpec.env.put( "DRILL_HOME", "/Users/progers/play/apache-drill-1.5.0" );
    workerSpec.command = "$DRILL_HOME/bin/drillbit.sh";
    workerSpec.cmdArgs.add("yarn_start");

    TaskSpec taskSpec = new TaskSpec();
    taskSpec.containerSpec = containerSpec;
    taskSpec.launchSpec = workerSpec;
    taskSpec.maxRetries = 10;

    Scheduler testGroup = new DrillbitScheduler("Basic-Drillbit", taskSpec, count);
    dispatcher.getController().registerScheduler(testGroup);

    dispatcher.registerPollable(new MockCommandPollable(dispatcher.getController()));
    zkConnect = "localhost:2181/drill/paul";
    ZKClusterCoordinatorDriver driver;
    try {
      driver = new ZKClusterCoordinatorDriver().setConnect(zkConnect).setFailureTimoutMs(30_000);
    } catch (ZKConfigException e) {
      throw new AMWrapperException("ZK setup failed", e);
    }
    ZKRegistry zkRegistry = new ZKRegistry(driver);
    dispatcher.registerAddOn(zkRegistry);
    dispatcher.getController().registerLifecycleListener( zkRegistry );

    return dispatcher;
  }

}
