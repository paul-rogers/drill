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

public class StartCommand extends ClientCommand
{
  public static class FileUploader
  {
    private Config config;
    FileSystem fs;
    private boolean localize;
    private Path destPath;
    private String localArchivePath;
    private File localArchiveFile;
    private boolean verbose;
    public Map<String, LocalResource> resources = new HashMap<>();

    public FileUploader( Config config, boolean verbose ) {
      this.config = config;
      this.verbose = verbose;
      localize = config.getBoolean( DrillOnYarnConfig.LOCALIZE_DRILL );
    }

    public void run( boolean upload ) throws ClientException {
      if ( ! localize ) {
        String drillHome = config.getString( DrillOnYarnConfig.DRILL_HOME );
        if ( DoYUtil.isBlank( drillHome ) ) {
          throw new ClientException( "Non-localized run but " + DrillOnYarnConfig.DRILL_HOME + " is not set." );
        }
        return;
      }
      setUploadPath( );
      if ( upload ) {
        connectToDfs( );
        uploadArchive( );
      }
      defineResources( );
    }

    private void connectToDfs( ) throws ClientException
    {
      String dfsConnection = config.getString( DrillOnYarnConfig.DFS_CONNECTION );
      Configuration conf = new YarnConfiguration();
      try {
        if ( verbose ) {
          System.out.print( "YARN Config: " );
          Configuration.dumpConfiguration(conf, new OutputStreamWriter( System.out ) );
          System.out.println( );
        }
      } catch (IOException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
      try {
        if ( DoYUtil.isBlank( dfsConnection ) ) {
          fs = FileSystem.get(conf);
        }
        else {
          URI uri;
          try {
            uri = new URI( dfsConnection );
          } catch (URISyntaxException e) {
            throw new ClientException( "Illformed DFS connection: " + dfsConnection, e );
          }
          fs = FileSystem.get(uri, conf);
        }
      } catch (IOException e) {
        throw new ClientException( "Failed to create the DFS", e );
      }
//      try {
//        RemoteIterator<LocatedFileStatus> iter = fs.listFiles( new Path( "/" ), true );
//        while ( iter.hasNext() ) {
//          LocatedFileStatus stat = iter.next();
//          System.out.println( stat.getPath() );
//        }
//      } catch (IllegalArgumentException | IOException e) {
//        // TODO Auto-generated catch block
//        e.printStackTrace();
//      }
    }

    private void setUploadPath() throws ClientException {
      String dfsDirStr = config.getString( DrillOnYarnConfig.DFS_APP_DIR );
      localArchivePath = config.getString( DrillOnYarnConfig.DRILL_ARCHIVE_PATH );
      localArchiveFile = new File( localArchivePath );
      String baseName = localArchiveFile.getName( );

      Path appDir;
      if ( dfsDirStr.startsWith( "/" ) ) {
        appDir = new Path( dfsDirStr );
      }
      else {
        Path home = fs.getHomeDirectory();
        appDir = new Path( home, dfsDirStr );
      }
      destPath = new Path( appDir, baseName );
    }

    public void uploadArchive( ) throws ClientException
    {
      // Create the application upload directory if it does not yet exist.

      String dfsDirStr = config.getString( DrillOnYarnConfig.DFS_APP_DIR );
      Path appDir = new Path( dfsDirStr );
      try {
        if ( ! fs.isDirectory( appDir ) ) {
          // TODO: Set permissions explicitly.
          FsPermission perm = FsPermission.getDirDefault();
          fs.mkdirs( appDir, perm );
        }
      } catch (IOException e) {
        throw new ClientException( "Failed to create DFS directory: " + dfsDirStr, e );
      }

      // Thoroughly check the Drill archive. Errors with the archive seem a likely
      // source of confusion, so provide detailed error messages for common cases.

      if ( DoYUtil.isBlank( localArchivePath ) ) {
        throw new ClientException( "Drill archive path (" + DrillOnYarnConfig.DRILL_ARCHIVE_PATH  + ") is not set." );
      }
      if ( ! localArchiveFile.exists() ) {
        throw new ClientException( "Drill archive not found: " + localArchivePath );
      }
      if ( ! localArchiveFile.canRead() ) {
        throw new ClientException( "Drill archive is not readable: " + localArchivePath );
      }
      if ( localArchiveFile.isDirectory() ) {
        throw new ClientException( "Drill archive cannot be a directory: " + localArchivePath );
      }

      // The file must be an archive type so YARN knows to extract its contents.

      String baseName = localArchiveFile.getName( );
      if ( DrillOnYarnConfig.findSuffix( baseName ) == null ) {
        throw new ClientException( "Drill archive must be .tar.gz, .tgz or .zip: " + baseName );
      }

      Path srcPath = new Path( localArchivePath );

      // Do the upload, replacing the old archive.

      if ( verbose ) {
        System.out.println( "Uploading " + baseName + "..." );
      }
      try {
        // TODO: Specify file permissions and owner.

        fs.copyFromLocalFile( false, true, srcPath, destPath );
      } catch (IOException e) {
        throw new ClientException( "Failed to upload Drill archive to DFS: " +
                                   localArchiveFile.getAbsolutePath() +
                                   " --> " + destPath, e );
      }
    }

    private void defineResources() throws ClientException {

      // Put the application archive, visible to only the application.
      // Because it is an archive, it will be expanded by YARN prior to launch
      // of the AM.

      addResource( destPath, "drill", LocalResourceType.ARCHIVE, LocalResourceVisibility.APPLICATION );
    }

    public void addResource( Path dfsPath, String localPath,
                             LocalResourceType type, LocalResourceVisibility visibility ) throws ClientException {
      resources.put( localPath, AppSpec.makeResource(fs, dfsPath, type, visibility));
    }
  }

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
      // set to the container directory, so we just need thd name
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

      master.classPath.add( "$HADOOP_YARN_HOME/share/hadoop/yarn/*" );

      // Do not add the YARN lib directory, it causes class conflicts
      // with the Jersey code in the AM.

//      master.classPath.add( "$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*" );

      // Followed by Drill override dependency jars

      master.classPath.add( drillHome + "jars/ext/*" );

      // Followed by other Drill dependency jars

      master.classPath.add( drillHome + "jars/3rdparty/*" );
      master.classPath.add( drillHome + "jars/classb/*" );

      // Finally any user specified

      String customCp = config.getString( DrillOnYarnConfig.AM_CLASSPATH );
      if ( ! DoYUtil.isBlank( customCp ) ) {
        master.classPath.add( customCp );
      }

      // AM main class

      master.mainClass = DrillApplicationMaster.class.getCanonicalName();

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
      System.out.println( "Launching " + master.appName + "..." );
      launchApp( master );
      writeAppIdFile( );
      waitForStartAndReport( );
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

    private class StartMonitor
    {
      ReportCommand.Reporter reporter;
      private YarnApplicationState state;

      void run( ) throws ClientException {
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
        System.out.print( "Application State: " );
        System.out.println( state.toString( ) );
        System.out.print( "Starting..." );
      }
    }

    private void waitForStartAndReport() throws ClientException {
      StartMonitor monitor = new StartMonitor( );
      monitor.run( );

//      ReportCommand.Reporter reporter = new ReportCommand.Reporter( appId, client );
//      int attempt = 0;
//      for ( ; ; ) {
//        attempt++;
//        if ( attempt > 15 ) {
//          System.out.println( );
//          System.out.println( "Application Master is slow to start, use the report command later to check status." );
//          break;
//        }
//        try {
//          reporter.getReport( );
//        }
//        catch ( ClientException e ) {
//          if ( attempt > 1 ) {
//            System.out.println( );
//          }
//          throw e;
//        }
//        if ( ! reporter.isStarting( ) ) {
//          if ( attempt > 1 ) {
//            System.out.println( );
//          }
//          reporter.run( verbose, true );
//          break;
//        }
//        try {
//          Thread.sleep( 1000 );
//        } catch (InterruptedException e) {
//          if ( attempt > 1 ) {
//            System.out.println( );
//          }
//          break;
//        }
//        if ( attempt == 1 ) {
//          System.out.print( "Starting..." );
//        }
//        else {
//          System.out.print( "." );
//        }
//      }
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
