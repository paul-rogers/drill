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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.drill.yarn.core.AppSpec;
import org.apache.drill.yarn.core.DrillOnYarnConfig;
import org.apache.drill.yarn.core.DoYUtil;
import org.apache.drill.yarn.core.YarnClientException;
import org.apache.drill.yarn.core.YarnRMClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;

import com.typesafe.config.Config;

public class StartCommand extends ClientCommand
{
  Config config;
  FileSystem fs;
  private Path destPath;
  private boolean upload;
  private boolean launch;
  private String localArchivePath;
  private File localArchiveFile;
  private boolean localize;
  private String drillHome;

  public StartCommand( boolean upload, boolean launch ) {
    this.upload = upload;
    this.launch = launch;
  }

  @Override
  public void run() throws ClientException
  {
    init( );
    setUploadPath( );
    if ( upload & localize ) {
      connectToDfs( );
      uploadArchive( );
    }
    if ( launch ) {
      launchAm( );
    }
  }

  private void init() {
    config = DrillOnYarnConfig.config();
  }

  private void connectToDfs( ) throws ClientException
  {
    String dfsConnection = config.getString( DrillOnYarnConfig.DFS_CONNECTION );
    Configuration conf = new Configuration();
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
    try {
      RemoteIterator<LocatedFileStatus> iter = fs.listFiles( new Path( "/" ), true );
      while ( iter.hasNext() ) {
        LocatedFileStatus stat = iter.next();
        System.out.println( stat.getPath() );
      }
    } catch (IllegalArgumentException | IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private void setUploadPath() throws ClientException {
    localize = config.getBoolean( DrillOnYarnConfig.LOCALIZE_DRILL );
    if ( ! localize ) {
      drillHome = config.getString( DrillOnYarnConfig.DRILL_HOME );
      if ( DoYUtil.isBlank( drillHome ) ) {
        throw new ClientException( "Non-localized run but " + DrillOnYarnConfig.DRILL_HOME + " is not set." );
      }
    }
    String dfsDirStr = config.getString( DrillOnYarnConfig.DFS_DIR );
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
    String dfsDirStr = config.getString( DrillOnYarnConfig.DFS_DIR );
    Path appDir = new Path( dfsDirStr );
    try {
      // TODO: Set permissions explicitly.
      FsPermission perm = FsPermission.getDirDefault();
      fs.mkdirs( appDir, perm );
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

    // Remove old archive.

//    try {
//      if ( fs.exists( destPath ) ) {
//        if ( fs.delete( destPath, false ) );
//      }
//    } catch (IOException e) {
//      throw new ClientException( "Failed to delete existing Drill archive on DFS: " + destPath, e );
//    }

    // Do the upload, replacing the old archive.

    if ( opts.verbose ) {
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

  private void launchAm( )
  {
    AppSpec master = new AppSpec();
    String value = config.getString( DrillOnYarnConfig.AM_VM_ARGS );
    if ( ! DoYUtil.isBlank( value ) ) {
      master.env.put( "DRILL_AM_JAVA_OPTS", value );
    }
    master.env.put( "DRILL_AM_HEAP", config.getString( DrillOnYarnConfig.AM_HEAP ) );
    if ( config.getBoolean( DrillOnYarnConfig.AM_DEBUG_LAUNCH ) ) {
      master.env.put( "DRILL_DEBUG", "1" );
    }
    value = config.getString( DrillOnYarnConfig.AM_CLASSPATH );
    if ( ! DoYUtil.isBlank( value ) ) {
      master.env.put( "DRILL_CLASSPATH", value );
    }

    String home = DrillOnYarnConfig.getDrillHome( config );
    master.command = home +  "/bin/" + "drill-am.sh";
    master.memoryMb = config.getInt( DrillOnYarnConfig.AM_MEMORY );
    master.vCores = config.getInt( DrillOnYarnConfig.AM_VCORES );
    master.appName = config.getString( DrillOnYarnConfig.APP_NAME );

    if ( opts.verbose ) {
      System.out.println( "Launching " + master.appName + "..." );
    }
    YarnRMClient client = new YarnRMClient();
    try {
      client.launchAppMaster(master);
      client.waitForCompletion();
    } catch (YarnClientException e) {
      e.printStackTrace();
    }
  }
}
