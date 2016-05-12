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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.drill.yarn.appMaster.DrillApplicationMaster;
import org.apache.drill.yarn.core.AppSpec;
import org.apache.drill.yarn.core.DfsFacade;
import org.apache.drill.yarn.core.DfsFacade.DfsFacadeException;
import org.apache.drill.yarn.core.DfsFacade.Localizer;
import org.apache.drill.yarn.core.DoYUtil;
import org.apache.drill.yarn.core.DrillOnYarnConfig;
import org.apache.drill.yarn.core.YarnClientException;
import org.apache.drill.yarn.core.YarnRMClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.typesafe.config.Config;

/**
 * Launches a drill cluster by uploading the Drill archive then launching the
 * Drill Application Master (AM). For testing, can also do just the upload or
 * just the launch. Handles both a localized Drill and a non-localized launch
 * (which uses a pre-installed Drill.)
 * <p>
 * This single operation combines upload and launch because the upload
 * Information is needed by the launch.
 * <p>
 * On the surface, it would seem that uploading a file and launching an app
 * should be simple operations. However, under YARN, we must handle a large
 * number of details that must be gotten exactly right. Plus, both the upload
 * and launch can be slow operations, so we provide feedback to the user that
 * something is, indeed, happening.
 */

public class StartCommand extends ClientCommand
{
  /**
   * Performs the file upload portion of the operation by uploading an
   * archive to the target DFS system and directory. Records the uploaded
   * archive so it may be used for localizing Drill in the launch step.
   * <p>
   * Some of the code is a bit of a dance so we can get information
   * early to display in status messages.
   */

  public static class FileUploader
  {
    private Config config;
    private DfsFacade dfs;
    private boolean verbose;

    public Map<String, LocalResource> resources = new HashMap<>();

    public FileUploader( Config config, boolean verbose ) {
      this.config = config;
      this.verbose = verbose;
    }

    public void run( boolean upload ) throws ClientException {
      dfs = new DfsFacade( config );
      if ( ! dfs.isLocalized() ) {
        String drillHome = config.getString( DrillOnYarnConfig.DRILL_HOME );
        if ( DoYUtil.isBlank( drillHome ) ) {
          throw new ClientException( "Non-localized run but " + DrillOnYarnConfig.DRILL_HOME + " is not set." );
        }
        return;
      }
      if ( verbose ) {
        dumpYarnConfig( );
      }
      DfsFacade.Localizer localizer = new DfsFacade.Localizer( dfs );

      connectToDfs( localizer, upload );
      if ( upload ) {
        upload( localizer );
      }
      try {
        resources = localizer.defineResources( );
      } catch (DfsFacadeException e) {
        throw new ClientException( "Failed to get DFS status for Drill archive", e );
      }
    }

    private void connectToDfs(DfsFacade.Localizer localizer, boolean upload) throws ClientException
    {
      // Print the progress message here because doing the connect takes
      // a while and the message makes it look like we're doing something.

      if ( upload ) {
        System.out.println( "Uploading " + localizer.getBaseName() + " to " + localizer.getDestPath() + " ..." );
      } else {
        System.out.println( "Checking uploaded file " + localizer.getDestPath() );
      }
      try {
        dfs.connect();
      } catch (DfsFacadeException e) {
        throw new ClientException( "Failed to connect to DFS", e );
      }
    }

    private void dumpYarnConfig() throws ClientException {
      try {
        System.out.print( "YARN Config: " );
        dfs.dumpYarnConfig( new OutputStreamWriter( System.out ) );
        System.out.println( );
      } catch (IOException e) {
        throw new ClientException( "Failed to dump YARN configuration", e );
      }
    }

    private void upload(Localizer localizer) throws ClientException {
      try {
        localizer.upload();
      } catch (DfsFacadeException e) {
        throw new ClientException( "Failed to upload Drill archive", e );
      }
      if ( verbose ) {
        System.out.println( "Uploaded." );
      }
    }
  }

  /**
   * Launch the AM through YARN. Builds the launch description, then tracks
   * the launch operation itself. Finally, provides the user with links to
   * track the AM both through YARN and via the AM's own web UI.
   */

  public static class AMRunner
  {
    private Config config;
    private boolean verbose;
    private ApplicationId appId;
    public Map<String, LocalResource> resources;
    private YarnRMClient client;

    public AMRunner( Config config, boolean verbose ) {
      this.config = config;
      this.verbose = verbose;
      client = new YarnRMClient();
    }

    public void run( boolean launch ) throws ClientException {
      if ( launch ) {
        AppSpec master = buildSpec( );
        launch( master );
      }
    }

    private AppSpec buildSpec( ) throws ClientException
    {
      AppSpec master = new AppSpec();

      // The drill home location is either a non-localized location,
      // or, more typically, the expanded Drill directory under the
      // container's working directory. When the localized directory,
      // we rely on the fact that the current working directory is
      // set to the container directory, so we just need the name
      // of the Drill folder under the cwd.

      String drillHome = DrillOnYarnConfig.getRemoteDrillHome( config ) + "/";
      String drillConf = drillHome + "conf";

      // Heap memory

      String heapMem = config.getString( DrillOnYarnConfig.AM_HEAP );
      master.vmArgs.add( "-Xms" + heapMem );
      master.vmArgs.add( "-Xmx" + heapMem );

      // Any additional VM arguments form the config file.

      String customVMArgs = config.getString( DrillOnYarnConfig.AM_VM_ARGS );
      if ( ! DoYUtil.isBlank( customVMArgs ) ) {
        master.vmArgs.add( customVMArgs );
      }

      // AM logging setup. Note: the Drillbit log file uses the default name
      // of logback.xml. So, we must tell the AM to use an AM-specific file
      // else we'll get warnings about the log.query.path system property
      // not being set (and we won't pick up the AM logging settings.)
      //
      // Note that there is no log file setting for the AM. YARN captures
      // stdout, and the AM produces moderate logging, so we just send
      // log output to stdout and let YARN capture it rather than creating
      // a separate log file.

      master.vmArgs.add( "-Dlogback.configurationFile=" + drillConf + "/drill-am-log.xml" );

      // Class path, assembled as per drill-config.sh (with simplifications
      // appropriate to the application master.)

      // Start with the YARN configuration directory (on the node
      // running the AM). Note that this directory MUST appear in the
      // class path before the drill config since Drill provides a
      // (empty) version of core-site.xml. Of course, if the user wants
      // to use the Drill version, they simply leave the Hadoop config
      // directory empty.
      //
      // The YARN config directory is usually the first entry in
      // client.getYarnAppClassPath( ), but we avoid using that path as it
      // includes other Hadoop jars which conflict with the jars included with
      // Drill.

      // Add Drill conf folder at the beginning of the classpath.
      // Note: for this to work, the core-site.xml file in $DRILL_HOME/conf
      // MUST be deleted or renamed. Drill 1.8 renames the file to
      // core-site-example.xml. But if the user simply copies all config
      // files from a previous release, then the AM startup will fail because
      // the Drill "dummy" core-site.xml will hide the "real" Hadoop
      // version, preventing YARN from initializing correctly.

      master.classPath.add( drillConf );

      // Followed by any user specified override jars

      String prefixCp = config.getString( DrillOnYarnConfig.AM_PREFIX_CLASSPATH );
      if ( ! DoYUtil.isBlank( prefixCp ) ) {
        master.classPath.add( prefixCp );
      }

      // Next Drill core jars

      master.classPath.add( drillHome + "jars/tools/*" );
      master.classPath.add( drillHome + "jars/*" );

      // Add the Hadoop config directory which we need to gain access to
      // YARN and HDFS. This is an odd location to add the config dir,
      // but if we add it sooner, Jersey complains with many class not
      // found errors for reasons not yet known. Note that, to add the
      // Hadoop config here, the Drill 1.6 $DRILL_HOME/conf/core-site.xml file
      // MUST have been removed or renamed else Hadoop will pick up
      // our dummy file instead of the real Hadoop file.

      master.classPath.add( "$HADOOP_CONF_DIR" );

      // Hadoop YARN jars.
      // We do this instead of using client.getYarnAppClassPath( ) because
      // we want to avoid Hadoop jars that conflict with Drill jars.
      // While it would seem more logical to put these at the END of the
      // class path, it turns out that we get unresolved class errors
      // from Jersey if we do. Putting YARN jars here works better.

      //master.classPath.add( "$HADOOP_YARN_HOME/share/hadoop/yarn/*" );

      // Do not add the YARN lib directory, it causes class conflicts
      // with the Jersey code in the AM.

//      master.classPath.add( "$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*" );

      // Followed by Drill override dependency jars

      master.classPath.add( drillHome + "jars/ext/*" );

      // Followed by other Drill dependency jars. We assume that the YARN jars
      // are replicated in the 3rdparty directory. Note that this requires that
      // Drill was built with the same version of YARN under which Drill-on-YARN
      // is to run.

      master.classPath.add( drillHome + "jars/3rdparty/*" );
      master.classPath.add( drillHome + "jars/classb/*" );

      // Finally any user specified

      String customCp = config.getString( DrillOnYarnConfig.AM_CLASSPATH );
      if ( ! DoYUtil.isBlank( customCp ) ) {
        master.classPath.add( customCp );
      }

      // AM main class

      master.mainClass = DrillApplicationMaster.class.getCanonicalName();

      // Localized resources

      master.resources.putAll( resources );

      // Container specification.

      master.memoryMb = config.getInt( DrillOnYarnConfig.AM_MEMORY );
      master.vCores = config.getInt( DrillOnYarnConfig.AM_VCORES );
      master.appName = config.getString( DrillOnYarnConfig.APP_NAME );
      master.queueName = config.getString( DrillOnYarnConfig.YARN_QUEUE );
      master.priority = config.getInt( DrillOnYarnConfig.YARN_PRIORITY );

      if ( verbose ) {
        System.out.println( "----------------------------------------------" );
        System.out.println( "Application Master Launch Spec" );
        master.dump( System.out );
        System.out.println( "----------------------------------------------" );
      }
      return master;
    }

    private void launch( AppSpec master ) throws ClientException
    {
      launchApp( master );
      writeAppIdFile( );
      waitForStartAndReport( master.appName );
    }

    private void launchApp( AppSpec master ) throws ClientException
    {
      GetNewApplicationResponse appResponse;
      try {
        appResponse = client.createAppMaster();
      } catch (YarnClientException e) {
        throw new ClientException( "Failed to allocate Drill application master", e );
      }
      appId = appResponse.getApplicationId();
      System.out.println("Application ID: " + appId.toString());

      // Memory and core checks per YARN app specs.

      int maxMemory = appResponse.getMaximumResourceCapability().getMemory();
      int maxCores = appResponse.getMaximumResourceCapability().getVirtualCores();
      if ( verbose ) {
        System.out.println("Max Memory: " + maxMemory);
        System.out.println("Max Cores: " + maxCores );
      }
      if ( master.memoryMb > maxMemory ) {
        throw new ClientException( "YARN maximum memory is " + maxMemory + " but the application master requests " + master.memoryMb );
      }
      if ( master.vCores > maxCores ) {
        throw new ClientException( "YARN maximum vcores is " + maxCores + " but the application master requests " + master.vCores );
      }

      try {
        client.submitAppMaster(master);
      } catch (YarnClientException e) {
        throw new ClientException( "Failed to start Drill application master", e );
      }
    }

    /**
     * Write the app id file needed for subsequent commands. The app id file
     * is the only way we know the YARN application associated with our Drill-on-YARN
     * session. This file is ready by subsequent status, resize and stop commands
     * so we can find our Drill AM on the YARN cluster.
     *
     * @throws ClientException
     */

    private void writeAppIdFile( ) throws ClientException
    {
      // Write the appid file that lets us work with the app later
      // (Analogous to a pid file.)
      // File goes into the directory above Drill Home (which should be the
      // folder that contains the localized archive) and is named for the
      // ZK cluster (to ensure that the name is a valid file name.)

      File appIdFile = ClientCommand.getAppIdFile( );
      try {
        PrintWriter writer = new PrintWriter( new FileWriter( appIdFile ) );
        writer.println( appId );
        writer.close( );
      }
      catch ( IOException e ) {
        throw new ClientException( "Failed to write appid file: " + appIdFile.getAbsolutePath() );
      }
    }

    /**
     * Poll YARN to track the launch process of the application so that we can
     * wait until the AM is live before pointing the user to the AM's
     * web UI.
     */

    private class StartMonitor
    {
      ReportCommand.Reporter reporter;
      private YarnApplicationState state;

      void run( String appName ) throws ClientException {
        System.out.print( "Launching " + appName + "..." );
        reporter = new ReportCommand.Reporter( client );
        reporter.getReport();
        if ( ! reporter.isStarting() ) {
          return; }
        updateState( reporter.getState( ) );
        try {
          for ( int attempt = 0;  attempt < 15;  attempt++ )
          {
            if ( ! poll( ) ) {
              break; }
          }
        } finally {
          System.out.println( );
        }
        reporter.display( verbose, true );
        if ( reporter.isStarting() ) {
          System.out.println( "Application Master is slow to start, use the report command later to check status." );
        }
      }

      private boolean poll( ) throws ClientException {
        try {
          Thread.sleep( 1000 );
        } catch (InterruptedException e) {
          return false;
        }
        reporter.getReport( );
        if ( ! reporter.isStarting( ) ) {
          return false;
        }
        YarnApplicationState newState = reporter.getState( );
        if ( newState == state ) {
          System.out.print( "." );
          return true;
        }
        System.out.println( );
        updateState( newState );
        return true;
      }

      private void updateState( YarnApplicationState newState ) {
        state = newState;
        if ( verbose ) {
          System.out.print( "Application State: " );
          System.out.println( state.toString( ) );
          System.out.print( "Starting..." );
        }
      }
    }

    private void waitForStartAndReport( String appName ) throws ClientException {
      StartMonitor monitor = new StartMonitor( );
      monitor.run( appName );
    }
  }

  private Config config;
  private boolean upload;
  private boolean launch;

  public StartCommand( boolean upload, boolean launch ) {
    this.upload = upload;
    this.launch = launch;
  }

  @Override
  public void run() throws ClientException
  {
    config = DrillOnYarnConfig.config();
    FileUploader uploader = new FileUploader( config, opts.verbose );
    uploader.run( upload );
    AMRunner runner = new AMRunner( config, opts.verbose );
    runner.resources = uploader.resources;
    runner.run( launch );
  }
}
