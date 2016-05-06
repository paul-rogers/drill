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
import org.apache.drill.yarn.core.DrillOnYarnConfig.Pool;
import org.apache.drill.yarn.core.LaunchSpec;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.yarn.core.DoYUtil;
import org.apache.drill.yarn.mock.MockCommandPollable;
import org.apache.drill.yarn.zk.ZKClusterCoordinatorDriver;
import org.apache.drill.yarn.zk.ZKConfigException;
import org.apache.drill.yarn.zk.ZKRegistry;
import org.apache.hadoop.yarn.api.ApplicationConstants;

import com.typesafe.config.Config;

public class DrillbitFactory implements ControllerFactory
{
  @Override
  public Dispatcher build() throws YarnFacadeException {

    Config config = DrillOnYarnConfig.config();

    TaskSpec taskSpec = buildDrillTaskSpec( config );

    // Prepare dispatcher

    Dispatcher dispatcher = new Dispatcher();
    int pollPeriodMs = config.getInt( DrillOnYarnConfig.AM_POLL_PERIOD_MS );
    int timerPeriodMs = config.getInt( DrillOnYarnConfig.AM_TICK_PERIOD_MS );
    AMYarnFacadeImpl yarn = new AMYarnFacadeImpl(pollPeriodMs, timerPeriodMs);
    dispatcher.setYarn(yarn);
    dispatcher.getController().setMaxRetries( config.getInt( DrillOnYarnConfig.DRILLBIT_MAX_RETRIES ) );

    // Assume basic scheduler for now.
    Pool pool = DrillOnYarnConfig.instance().getPool( 0 );
    Scheduler testGroup = new DrillbitScheduler(pool.name, taskSpec, pool.count);
    dispatcher.getController().registerScheduler(testGroup);

    // Dummy API for now.

    dispatcher.registerPollable(new MockCommandPollable(dispatcher.getController()));

    // ZooKeeper setup

    buildZooKeeper( config, dispatcher );

    // Tracking Url
    // TODO: HTTPS support

    String trackingUrl = null;
    if ( config.getBoolean( DrillOnYarnConfig.HTTP_ENABLED ) ) {
      trackingUrl = "http://<host>:<port>/";
      trackingUrl = trackingUrl.replace( "<port>", Integer.toString( config.getInt( DrillOnYarnConfig.HTTP_PORT ) ) );
      dispatcher.setTrackingUrl(trackingUrl);
    }

    return dispatcher;
  }

  /**
   * Constructs the Drill launch command. Performs the equivalent of
   * drillbit.sh, drill-config.sh, drill-env.sh and runbit.
   * <p>
   * We launch Drill directly from YARN (rather than indirectly via a
   * script) because of the need to have the node manager kill Drill
   * when shutting down a Drill-bit. To do this, the RM keeps track
   * of the pid of the process that it launches. When using a script
   * that pid is different than the pid of the Drillbit itself, resulting
   * in a zombie Drill-bit and a failure of the Drillbit to actually
   * shut down, which shows up in the failure to receive a ZK
   * deregistration event. This is true even if our script uses "exec"
   * to launch Drill. However, if we launch Drill (actually Java)
   * directly, then the pids work and the NM can kill Drill correctly.
   * The cost is that we have to change this code to change the launch
   * environment or command.
   * <p>
   * This is an exercise in getting
   * many details just right. The easiest way to understand this code is
   * to insert an "echo" statement in drill-bit.sh to echo the launch
   * command there. Then, look in YARN's NM private container directory
   * for the launch_container.sh script to see the command generated
   * by the following code. Compare the two to validate that the code
   * does the right thing.
   * <p>
   * This class is very Linux-specific. The usual adjustments must
   * be made to adapt it to Windows.
   *
   * @param config
   * @return
   */

  private TaskSpec buildDrillTaskSpec(Config config) {

    // Drillbit launch description

    ContainerRequestSpec containerSpec = new ContainerRequestSpec();
    containerSpec.memoryMb = config.getInt( DrillOnYarnConfig.DRILLBIT_MEMORY );
    containerSpec.vCores = config.getInt( DrillOnYarnConfig.DRILLBIT_VCORES );

    // Heap memory

    LaunchSpec workerSpec = new LaunchSpec();
    String heapMem = config.getString( DrillOnYarnConfig.DRILLBIT_HEAP );
    workerSpec.vmArgs.add( "-Xms" + heapMem );
    workerSpec.vmArgs.add( "-Xmx" + heapMem );

    // Direct memory

    String directMem = config.getString( DrillOnYarnConfig.DRILLBIT_DIRECT_MEM );
    workerSpec.vmArgs.add( "-XX:MaxDirectMemorySize=" + directMem );

    // Other VM options.
    // From dril;-env.sh

    workerSpec.vmArgs.add( "-XX:MaxPermSize=512M" );
    workerSpec.vmArgs.add( "-XX:ReservedCodeCacheSize=1G" );
    workerSpec.vmArgs.add( "-Ddrill.exec.enable-epoll=true" );
    workerSpec.vmArgs.add( "-XX:+UseG1GC" );

    // Class unloading is disabled by default in Java 7
    // http://hg.openjdk.java.net/jdk7u/jdk7u60/hotspot/file/tip/src/share/vm/runtime/globals.hpp#l1622

    workerSpec.vmArgs.add( "-XX:+CMSClassUnloadingEnabled" );

    // Any additional VM arguments form the config file.

    String customVMArgs = config.getString( DrillOnYarnConfig.DRILLBIT_VM_ARGS );
    if ( ! DoYUtil.isBlank( customVMArgs ) ) {
      workerSpec.vmArgs.add( customVMArgs );
    }

    // Drill logs.
    // Relies on the LOG_DIR_EXPANSION_VAR marker which is replaced by
    // the container log directory.

    String logDir = ApplicationConstants.LOG_DIR_EXPANSION_VAR;
    workerSpec.vmArgs.add( "-Dlog.path=" + logDir + "/drillbit.log" );
    workerSpec.vmArgs.add( "-Dlog.query.path=" + logDir + "/drillbit_queries.json" );

    // Garbage collection (gc) logging. In drillbit.sh logging can be
    // configured to go anywhere. In YARN, all logs go to the YARN log
    // directory; the gc log file is always called "gc.log".

    if ( config.getBoolean( DrillOnYarnConfig.DRILLBIT_LOG_GC ) ) {
      workerSpec.vmArgs.add( "-Xloggc:" + logDir + "/gc.log" );
    }

    // Class path, assembled as per drill-config.sh.
    // The drill home location is either a non-localized location,
    // or, more typically, the expanded Drill directory under the
    // container's working directory. When the localized directory,
    // we rely on the fact that the current working directory is
    // set to the container directory, so we just need thd name
    // of the Drill folder under the cwd.

    String drillHome = DrillOnYarnConfig.getRemoteDrillHome( config ) + "/";

    // Add Drill conf folder at the beginning of the classpath

    workerSpec.classPath.add( drillHome + "conf" );

    // Followed by any user specified override jars

    String prefixCp = config.getString( DrillOnYarnConfig.DRILLBIT_PREFIX_CLASSPATH );
    if ( ! DoYUtil.isBlank( prefixCp ) ) {
      workerSpec.classPath.add( prefixCp );
    }

    // Next Drill core jars

    workerSpec.classPath.add( drillHome + "jars/*" );

    // Followed by Drill override dependency jars

    workerSpec.classPath.add( drillHome + "jars/ext/*" );

    // Followed by Hadoop's jar, HBase' jar. Generalized
    // here to a class-path of external jars set in the config.

    String extnCp = config.getString( DrillOnYarnConfig.DRILLBIT_EXTN_CLASSPATH );
    if ( ! DoYUtil.isBlank( extnCp ) ) {
      workerSpec.classPath.add( extnCp );
    }

    // Followed by other Drill dependency jars

    workerSpec.classPath.add( drillHome + "jars/3rdparty/*" );
    workerSpec.classPath.add( drillHome + "jars/classb/*" );

    // Finally any user specified

    String customCp = config.getString( DrillOnYarnConfig.DRILLBIT_CLASSPATH );
    if ( ! DoYUtil.isBlank( customCp ) ) {
      workerSpec.classPath.add( customCp );
    }

    // Note that there is no equivalent of niceness for YARN: YARN controls
    // the niceness of its child processes.

    // Drillbit main class (from runbit)

    workerSpec.mainClass = Drillbit.class.getCanonicalName();

    TaskSpec taskSpec = new TaskSpec();
    taskSpec.containerSpec = containerSpec;
    taskSpec.launchSpec = workerSpec;
    taskSpec.maxRetries = config.getInt( DrillOnYarnConfig.DRILLBIT_MAX_RETRIES );
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
