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

import org.apache.drill.yarn.mock.MockCommandPollable;
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
  public Dispatcher build() throws YarnFacadeException {
    Dispatcher dispatcher = new Dispatcher();
    AMYarnFacadeImpl yarn = new AMYarnFacadeImpl(pollPeriodMs, timerPeriodMs);
    dispatcher.setYarn(yarn);

    Scheduler testGroup = new DrillbitScheduler(count);
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
