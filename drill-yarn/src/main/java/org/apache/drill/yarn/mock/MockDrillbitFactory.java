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

public class MockDrillbitFactory implements ControllerFactory {
  int count;

  public MockDrillbitFactory() {
    this(2);
  }

  public MockDrillbitFactory(int count) {
    this.count = count;
  }

  @Override
  public Dispatcher build() throws ControllerFactoryException {
    Dispatcher dispatcher = new Dispatcher(2000);
    AMYarnFacadeImpl yarn = new AMYarnFacadeImpl(1000);
    try {
      dispatcher.setYarn(yarn);
    } catch (YarnFacadeException e) {
      throw new ControllerFactoryException( "Yarn Setup failed", e );
    }

    Scheduler testGroup = new MockDrillbitScheduler(count);
    dispatcher.getController().registerScheduler(testGroup);

    dispatcher.registerPollable(new MockCommandPollable(dispatcher.getController()));

    return dispatcher;
  }

}
