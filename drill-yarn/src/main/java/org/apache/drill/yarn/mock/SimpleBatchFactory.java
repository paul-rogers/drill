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

import org.apache.drill.yarn.appMaster.AMYarnFacadeImpl;
import org.apache.drill.yarn.appMaster.ControllerFactory;
import org.apache.drill.yarn.appMaster.Dispatcher;
import org.apache.drill.yarn.appMaster.Scheduler;
import org.apache.drill.yarn.appMaster.YarnFacadeException;
import org.apache.drill.yarn.appMaster.ControllerFactory.ControllerFactoryException;

/**
 * This tests setup starts and monitors n trivial tasks (such as the bin/date
 * shell command.) This design is overkill for such a simple application, but it
 * lays the groundwork for a managing a cluster of long-lived processes. This
 * test is a version of the YARN Hello World
 * <a href="https://github.com/hortonworks/simple-yarn-app">Simple Yarn App</a>.
 */

public class SimpleBatchFactory implements ControllerFactory
{

  private String command;
  private int count;

  public SimpleBatchFactory() {
    this("/bin/date", 2);
  }

  public SimpleBatchFactory(String command, int count) {
    this.command = command;
    this.count = count;
  }

  @Override
  public Dispatcher build() throws ControllerFactoryException {
    Dispatcher dispatcher = new Dispatcher();
    AMYarnFacadeImpl yarn = new AMYarnFacadeImpl(1000, 2000);
    try {
      dispatcher.setYarn(yarn);
    } catch (YarnFacadeException e) {
      throw new ControllerFactoryException( "Yarn Setup failed", e );
    }

    Scheduler testGroup = new TestBatchScheduler(command, count);
    dispatcher.getController().registerScheduler(testGroup);
    return dispatcher;
  }

}
