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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.drill.yarn.core.DfsFacade;
import org.apache.drill.yarn.core.DfsFacade.DfsFacadeException;
import org.apache.drill.yarn.core.DoYUtil;
import org.apache.drill.yarn.core.DoyConfigException;
import org.apache.drill.yarn.core.DrillOnYarnConfig;
import org.apache.drill.yarn.core.DrillOnYarnConfig.Pool;
import org.apache.drill.yarn.core.LaunchSpec;
import org.apache.drill.yarn.zk.ZKClusterCoordinatorDriver;
import org.apache.drill.yarn.zk.ZKConfigException;
import org.apache.drill.yarn.zk.ZKRegistry;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.LocalResource;

import com.typesafe.config.Config;

/**
 * Builds a controller for a cluster of Drillbits. The AM is designed
 * to be mostly generic; only this class contains knowledge that the
 * tasks being managed are drillbits. This design ensures that we can
 * add other Drill components in the future without the need to make
 * major changes to the AM logic.
 * <p>
 * The controller consists of a generic dispatcher and cluster controller,
 * along with a Drill-specific scheduler and task launch specification.
 * Drill also includes an interface to ZooKeeper to monitor Drillbits.
 * <p>
 * The AM is launched by YARN. All it knows is what is in its launch
 * environment or configuration files. The client must set up all the
 * information that the AM needs. Static information appears in configuration
 * files. But, dynamic information (or that which is inconvenient to repeat
 * in configuration files) must arrive in environment variables.
 * See {@link DrillOnYarnConfig} for more information.
 */

public class DrillControllerFactory implements ControllerFactory
{
  private static final Log LOG = LogFactory.getLog(DrillControllerFactory.class);
  private Config config = DrillOnYarnConfig.config();
  private String drillArchivePath;
  private String siteArchivePath;
  private boolean localized;

  @Override
  public Dispatcher build() throws ControllerFactoryException
  {
    Dispatcher dispatcher;
    try {
      Map<String, LocalResource> resources = prepareResources( );

      TaskSpec taskSpec = buildDrillTaskSpec( resources );

      // Prepare dispatcher

      dispatcher = new Dispatcher();
      int pollPeriodMs = config.getInt( DrillOnYarnConfig.AM_POLL_PERIOD_MS );
      int timerPeriodMs = config.getInt( DrillOnYarnConfig.AM_TICK_PERIOD_MS );
      AMYarnFacadeImpl yarn = new AMYarnFacadeImpl(pollPeriodMs, timerPeriodMs);
      dispatcher.setYarn(yarn);
      dispatcher.getController().setMaxRetries( config.getInt( DrillOnYarnConfig.DRILLBIT_MAX_RETRIES ) );

      // Assume basic scheduler for now.
      Pool pool = DrillOnYarnConfig.instance().getPool( 0 );
      Scheduler testGroup = new DrillbitScheduler(pool.name, taskSpec, pool.count);
      dispatcher.getController().registerScheduler(testGroup);

      // ZooKeeper setup

      buildZooKeeper( config, dispatcher );
    } catch (YarnFacadeException | DoyConfigException e) {
      throw new ControllerFactoryException( "Drill AM intitialization failed", e );
    }

    // Tracking Url
    // TODO: HTTPS support

    String trackingUrl = null;
    if ( config.getBoolean( DrillOnYarnConfig.HTTP_ENABLED ) ) {
      trackingUrl = "http://<host>:<port>/redirect";
      trackingUrl = trackingUrl.replace( "<port>", Integer.toString( config.getInt( DrillOnYarnConfig.HTTP_PORT ) ) );
      dispatcher.setTrackingUrl(trackingUrl);
    }

    return dispatcher;
  }

  private Map<String, LocalResource> prepareResources( ) throws YarnFacadeException {
    try {
      DfsFacade dfs = new DfsFacade( config );
      localized = dfs.isLocalized();
      if ( ! localized ) {
        return null;
      }
      dfs.connect();
      Map<String, LocalResource> resources = new HashMap<>( );
      DrillOnYarnConfig drillConfig = DrillOnYarnConfig.instance();

      // Localize the Drill archive.

      drillArchivePath = drillConfig.getDrillArchiveDfsPath( );
      DfsFacade.Localizer localizer = new DfsFacade.Localizer( dfs, drillArchivePath );
      String key = config.getString( DrillOnYarnConfig.DRILL_ARCHIVE_KEY );
      localizer.defineResources( resources, key );
      LOG.info( "Localizing " + drillArchivePath + " with key" + key );

      // Localize the site archive, if any.

      siteArchivePath = drillConfig.getSiteArchiveDfsPath( );
      if ( siteArchivePath != null ) {
        localizer = new DfsFacade.Localizer( dfs, siteArchivePath );
        key = config.getString( DrillOnYarnConfig.SITE_ARCHIVE_KEY );
        localizer.defineResources( resources, key );
        LOG.info( "Localizing " + siteArchivePath + " with key" + key );
      }
      return resources;
    } catch (DfsFacadeException e) {
      throw new YarnFacadeException( "Failed to get DFS status for Drill archive", e );
    }
  }

  /**
   * Constructs the Drill launch command. Performs the equivalent of
   * drillbit.sh, drill-config.sh, drill-env.sh and runbit.
   * <p>
   * We launch Drill directly from YARN (rather than indirectly via a
   * script) because doing so is more in the spirit of YARN and using
   * a script simply introduces another layer of complexity.
   * The cost is that we have to change this code to change the launch
   * environment or command.
   * <p>
   * This is an exercise in getting
   * many details just right. The code here sets the environment variables
   * required by (and documented in) yarn-drillbit.sh. The easiest way to
   * understand this code is
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
   * @throws DoyConfigException
   */

  private TaskSpec buildDrillTaskSpec(Map<String, LocalResource> resources) throws DoyConfigException
  {
    DrillOnYarnConfig doyConfig = DrillOnYarnConfig.instance( );

    // Drillbit launch description

    ContainerRequestSpec containerSpec = new ContainerRequestSpec();
    containerSpec.memoryMb = config.getInt( DrillOnYarnConfig.DRILLBIT_MEMORY );
    containerSpec.vCores = config.getInt( DrillOnYarnConfig.DRILLBIT_VCORES );
    containerSpec.disks = config.getDouble( DrillOnYarnConfig.DRILLBIT_DISKS );

    LaunchSpec drillbitSpec = new LaunchSpec();

    // The drill home location is either a non-localized location,
    // or, more typically, the expanded Drill directory under the
    // container's working directory. When the localized directory,
    // we rely on the fact that the current working directory is
    // set to the container directory, so we just need the name
    // of the Drill folder under the cwd.

    String drillHome = doyConfig.getRemoteDrillHome( );
    drillbitSpec.env.put( "DRILL_HOME", drillHome );
    LOG.trace( "Drillbit DRILL_HOME: " + drillHome );

    // Heap memory

    addIfSet( drillbitSpec, DrillOnYarnConfig.DRILLBIT_HEAP, "DRILL_HEAP" );

    // Direct memory

    addIfSet( drillbitSpec, DrillOnYarnConfig.DRILLBIT_DIRECT_MEM, "DRILL_MAX_DIRECT_MEMORY" );

    // Any additional VM arguments form the config file.

    addIfSet( drillbitSpec, DrillOnYarnConfig.DRILLBIT_VM_ARGS, "DRILL_JVM_OPTS" );

    // Drill logs.
    // Relies on the LOG_DIR_EXPANSION_VAR marker which is replaced by
    // the container log directory.

    if ( ! config.getBoolean( DrillOnYarnConfig.DISABLE_YARN_LOGS ) ) {
      drillbitSpec.env.put( "DRILL_YARN_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR );
    }

    // Debug option.

    if ( config.getBoolean( DrillOnYarnConfig.DRILLBIT_DEBUG_LAUNCH ) ) {
      drillbitSpec.env.put( "DRILL_DEBUG", "1" );
    }

    // Hadoop home should be set in drill-env.sh since it is needed
    // for client launch as well as the AM.

//    addIfSet( drillbitSpec, DrillOnYarnConfig.HADOOP_HOME, "HADOOP_HOME" );

    // When localized, the config directory must be $DRILL_HOME/conf, which
    // contains the localized files. We set this explicitly to prevent
    // drill-config.sh from using any of the default locations.
    // Note that we can do the obvious: $DRILL_HOME/conf, because env vars
    // are set in random order, one value set here can't rely on another.

//    if ( config.getBoolean( DrillOnYarnConfig.LOCALIZE_DRILL ) ) {
//      drillbitSpec.env.put( "DRILL_CONF_DIR", drillHome + "/conf" );
//    }

    // Garbage collection (gc) logging. In drillbit.sh logging can be
    // configured to go anywhere. In YARN, all logs go to the YARN log
    // directory; the gc log file is always called "gc.log".

    if ( config.getBoolean( DrillOnYarnConfig.DRILLBIT_LOG_GC ) ) {
      drillbitSpec.env.put( "ENABLE_GC_LOG", "1" );
    }

    // Class path additions.

    addIfSet( drillbitSpec, DrillOnYarnConfig.DRILLBIT_PREFIX_CLASSPATH, "DRILL_CLASSPATH_PREFIX" );
    addIfSet( drillbitSpec, DrillOnYarnConfig.DRILLBIT_CLASSPATH, "DRILL_CLASSPATH" );

    // Drill-config.sh has specific entries for Hadoop and Hbase. To prevent
    // an endless number of such one-off cases, we add a general extension
    // class path. But, we retain Hadoop and Hbase for backward compatibility.

    addIfSet( drillbitSpec, DrillOnYarnConfig.DRILLBIT_EXTN_CLASSPATH, "EXTN_CLASSPATH" );
    addIfSet( drillbitSpec, DrillOnYarnConfig.HADOOP_CLASSPATH, "DRILL_HADOOP_CLASSPATH" );
    addIfSet( drillbitSpec, DrillOnYarnConfig.HBASE_CLASSPATH, "DRILL_HBASE_CLASSPATH" );

    // Note that there is no equivalent of niceness for YARN: YARN controls
    // the niceness of its child processes.

    // Drillbit launch script under YARN
    // Here we can use DRILL_HOME because all env vars are set before
    // issuing this command.

    drillbitSpec.command = "$DRILL_HOME/bin/yarn-drillbit.sh";

    // Configuration (site directory), if given.

    String siteDirPath = doyConfig.getRemoteSiteDir();
    if ( siteDirPath != null ) {
      drillbitSpec.cmdArgs.add( "--site" );
      drillbitSpec.cmdArgs.add( siteDirPath );
    }

    // Localized resources

    if ( resources != null ) {
      drillbitSpec.resources.putAll( resources );
    }

    // Container definition.

    TaskSpec taskSpec = new TaskSpec();
    taskSpec.name = "Drillbit";
    taskSpec.containerSpec = containerSpec;
    taskSpec.launchSpec = drillbitSpec;
    taskSpec.maxRetries = config.getInt( DrillOnYarnConfig.DRILLBIT_MAX_RETRIES );
    return taskSpec;
  }

  public void addIfSet( LaunchSpec spec, String configParam, String envVar ) {
    String value = config.getString( configParam );
    if ( ! DoYUtil.isBlank( value ) ) {
      spec.env.put( envVar, value );
    }
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
