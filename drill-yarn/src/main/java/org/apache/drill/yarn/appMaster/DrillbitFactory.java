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

import org.apache.drill.yarn.core.DrillOnYarnConfig;
import org.apache.drill.yarn.core.LaunchSpec;
import org.apache.drill.yarn.core.DoYUtil;
import org.apache.drill.yarn.mock.MockCommandPollable;
import org.apache.drill.yarn.zk.ZKClusterCoordinatorDriver;
import org.apache.drill.yarn.zk.ZKConfigException;
import org.apache.drill.yarn.zk.ZKRegistry;

import com.typesafe.config.Config;

public class DrillbitFactory implements ControllerFactory
{
  @Override
  public Dispatcher build() throws YarnFacadeException {

    Config config = DrillOnYarnConfig.config();
    int pollPeriodMs = config.getInt( DrillOnYarnConfig.AM_POLL_PERIOD_MS );
    int timerPeriodMs = config.getInt( DrillOnYarnConfig.AM_TICK_PERIOD_MS );

    TaskSpec taskSpec = buildDrillTaskSpec( config );

    // Prepare dispatcher

    Dispatcher dispatcher = new Dispatcher();
    AMYarnFacadeImpl yarn = new AMYarnFacadeImpl(pollPeriodMs, timerPeriodMs);
    dispatcher.setYarn(yarn);

    // Assume basic scheduler for now.
    int count = config.getInt( DrillOnYarnConfig.poolKey( 0, DrillOnYarnConfig.POOL_SIZE ) );
    Scheduler testGroup = new DrillbitScheduler(taskSpec, count);
    dispatcher.getController().registerScheduler(testGroup);

    // Dummy API for now.

    dispatcher.registerPollable(new MockCommandPollable(dispatcher.getController()));

    // ZooKeeper setup

    buildZooKeeper( config, dispatcher );

    return dispatcher;
  }

  private TaskSpec buildDrillTaskSpec(Config config) {

    // Drillbit launch description

    ContainerRequestSpec containerSpec = new ContainerRequestSpec();
    containerSpec.memoryMb = config.getInt( DrillOnYarnConfig.DRILLBIT_MEMORY );
    containerSpec.vCores = config.getInt( DrillOnYarnConfig.DRILLBIT_VCORES );

    LaunchSpec workerSpec = new LaunchSpec();
    String home = DrillOnYarnConfig.getDrillHome( config );
    workerSpec.command = home + "/bin/yarn-drillbit.sh";

    workerSpec.env.put( "DRILL_HOME", home );
    String value = config.getString( DrillOnYarnConfig.AM_VM_ARGS );
    if ( ! DoYUtil.isBlank( value ) ) {
      workerSpec.env.put( "DRILL_AM_JAVA_OPTS", value );
    }
    workerSpec.env.put( "DRILL_HEAP", config.getString( DrillOnYarnConfig.DRILLBIT_HEAP ) );
    workerSpec.env.put( "DRILL_MAX_DIRECT_MEMORY", config.getString( DrillOnYarnConfig.DRILLBIT_DIRECT_MEM ) );
    if ( config.getBoolean( DrillOnYarnConfig.DRILLBIT_LOG_GC ) ) {
      workerSpec.env.put( "DRILL_LOG_GC", "1" );
    }
    workerSpec.env.put( "DRILLBIT_NICENESS", config.getString( DrillOnYarnConfig.DRILLBIT_NICENESS ) );
    value = config.getString( DrillOnYarnConfig.DRILLBIT_CLASSPATH );
    if ( ! DoYUtil.isBlank( value ) ) {
      workerSpec.env.put( "DRILL_CLASSPATH", value );
    }

    TaskSpec taskSpec = new TaskSpec();
    taskSpec.containerSpec = containerSpec;
    taskSpec.launchSpec = workerSpec;
    taskSpec.maxRetries = 10;
    return taskSpec;
  }

  private void buildZooKeeper(Config config, Dispatcher dispatcher) {
    ZKClusterCoordinatorDriver driver;
    try {
      String zkConnect = config.getString( DrillOnYarnConfig.ZK_CONNECT );
      String zkRoot = config.getString( DrillOnYarnConfig.ZK_ROOT );
      String clusterId = config.getString( DrillOnYarnConfig.CLUSTER_ID );
      String connectStr = zkConnect + "/" + zkRoot + "/" + clusterId;
      int failureTimeoutMs = config.getInt( DrillOnYarnConfig.ZK_FAILURE_TIMEOUT_MS );
      int retryCount =  config.getInt( DrillOnYarnConfig.ZK_RETRY_COUNT );
      int retryDelayMs =  config.getInt( DrillOnYarnConfig.ZK_RETRY_DELAY_MS );
      driver = new ZKClusterCoordinatorDriver()
          .setConnect(connectStr)
          .setFailureTimoutMs(failureTimeoutMs)
          .setRetryCount( retryCount )
          .setRetryDelayMs( retryDelayMs );
    } catch (ZKConfigException e) {
      throw new AMWrapperException("ZK setup failed", e);
    }
    ZKRegistry zkRegistry = new ZKRegistry(driver);
    dispatcher.registerAddOn(zkRegistry);
    dispatcher.getController().registerLifecycleListener(zkRegistry);
  }

}
