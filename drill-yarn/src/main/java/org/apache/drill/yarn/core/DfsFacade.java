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
package org.apache.drill.yarn.core;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.typesafe.config.Config;

/**
 * Facade to the distributed file system (DFS) system that implements
 * Drill-on-YARN related operations. Some operations are used by both the
 * client and AM applications.
 */

public class DfsFacade
{
  public static class DfsFacadeException extends Exception
  {
    private static final long serialVersionUID = 1L;

    public DfsFacadeException(String msg) {
      super( msg );
    }

    public DfsFacadeException(String msg, Exception e) {
      super( msg, e );
    }
  }

  private FileSystem fs;
  private Configuration yarnConf;
  private Config config;
  private boolean localize;

  public DfsFacade( Config config ) {
    this.config = config;
    localize = config.getBoolean( DrillOnYarnConfig.LOCALIZE_DRILL );
  }

  public boolean isLocalized( ) { return localize; }

  public void dumpYarnConfig( OutputStreamWriter out ) throws IOException {
    loadYarnConfig( );
    Configuration.dumpConfiguration( yarnConf, out );
  }

  public void connect( ) throws DfsFacadeException
  {
    loadYarnConfig( );
    String dfsConnection = config.getString( DrillOnYarnConfig.DFS_CONNECTION );
    try {
      if ( DoYUtil.isBlank( dfsConnection ) ) {
        fs = FileSystem.get(yarnConf);
      }
      else {
        URI uri;
        try {
          uri = new URI( dfsConnection );
        } catch (URISyntaxException e) {
          throw new DfsFacadeException( "Illformed DFS connection: " + dfsConnection, e );
        }
        fs = FileSystem.get(uri, yarnConf);
      }
    } catch (IOException e) {
      throw new DfsFacadeException( "Failed to create the DFS", e );
    }
//    try {
//      RemoteIterator<LocatedFileStatus> iter = fs.listFiles( new Path( "/" ), true );
//      while ( iter.hasNext() ) {
//        LocatedFileStatus stat = iter.next();
//        System.out.println( stat.getPath() );
//      }
//    } catch (IllegalArgumentException | IOException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    }
  }

  /**
   * Lazy loading of YARN configuration since it takes a long time
   * to load. (YARN provides no caching, sadly.
   */

  private void loadYarnConfig( ) {
    if ( yarnConf == null ) {
      yarnConf = new YarnConfiguration();
    }
  }

  public static class Localizer
  {
    private final DfsFacade dfs;
    protected File localArchivePath;
    protected Path dfsArchivePath;

    /**
     * Resources to be localized (downloaded) to each AM or drillbit
     * node.
     */


    public Localizer( DfsFacade dfs )
    {
      this.dfs = dfs;
      localArchivePath = dfs.getLocalPath( DrillOnYarnConfig.DRILL_ARCHIVE_PATH );
      dfsArchivePath = dfs.getUploadPath( localArchivePath );
    }

    public String getBaseName( ) {
      return localArchivePath.getName();
    }

    public String getDestPath( ) {
      return dfsArchivePath.toString();
    }

    public void upload( ) throws DfsFacadeException {
      dfs.uploadArchive( localArchivePath, dfsArchivePath );
    }

    public Map<String, LocalResource> defineResources() throws DfsFacadeException
    {
      Map<String, LocalResource> resources = new HashMap<>();

      // Put the application archive, visible to only the application.
      // Because it is an archive, it will be expanded by YARN prior to launch
      // of the AM.

      LocalResource drillResource = dfs.makeResource(dfsArchivePath,
                                                     LocalResourceType.ARCHIVE,
                                                     LocalResourceVisibility.APPLICATION);
      resources.put( DrillOnYarnConfig.LOCAL_DIR_NAME, drillResource );

      // Eventually, put configuration files and other customer-specific
      // items in a separate archive. They are combined for now.

      return resources;
    }
  }

  public File getLocalPath( String localPathParam ) {
    return new File( config.getString( localPathParam ) );
  }

  public Path getUploadPath( File localArchiveFile ) {
    String dfsDirStr = config.getString( DrillOnYarnConfig.DFS_APP_DIR );
    String baseName = localArchiveFile.getName( );

    Path appDir;
    if ( dfsDirStr.startsWith( "/" ) ) {
      appDir = new Path( dfsDirStr );
    }
    else {
      Path home = fs.getHomeDirectory();
      appDir = new Path( home, dfsDirStr );
    }
    return new Path( appDir, baseName );
  }

  public void uploadArchive( File localArchiveFile, Path destPath ) throws DfsFacadeException
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
      throw new DfsFacadeException( "Failed to create DFS directory: " + dfsDirStr, e );
    }

    // Thoroughly check the Drill archive. Errors with the archive seem a likely
    // source of confusion, so provide detailed error messages for common cases.

    String localArchivePath = config.getString( DrillOnYarnConfig.DRILL_ARCHIVE_PATH );
    if ( DoYUtil.isBlank( localArchivePath ) ) {
      throw new DfsFacadeException( "Drill archive path (" + DrillOnYarnConfig.DRILL_ARCHIVE_PATH  + ") is not set." );
    }
    if ( ! localArchiveFile.exists() ) {
      throw new DfsFacadeException( "Drill archive not found: " + localArchivePath );
    }
    if ( ! localArchiveFile.canRead() ) {
      throw new DfsFacadeException( "Drill archive is not readable: " + localArchivePath );
    }
    if ( localArchiveFile.isDirectory() ) {
      throw new DfsFacadeException( "Drill archive cannot be a directory: " + localArchivePath );
    }

    // The file must be an archive type so YARN knows to extract its contents.

    String baseName = localArchiveFile.getName( );
    if ( DrillOnYarnConfig.findSuffix( baseName ) == null ) {
      throw new DfsFacadeException( "Drill archive must be .tar.gz, .tgz or .zip: " + baseName );
    }

    Path srcPath = new Path( localArchivePath );

    // Do the upload, replacing the old archive.

    try {
      // TODO: Specify file permissions and owner.

      fs.copyFromLocalFile( false, true, srcPath, destPath );
    } catch (IOException e) {
      throw new DfsFacadeException( "Failed to upload Drill archive to DFS: " +
                                    localArchiveFile.getAbsolutePath() +
                                    " --> " + destPath, e );
    }
  }

  /**
   * Create a local resource definition for YARN. A local resource is one that
   * must be localized onto the remote node prior to running a command on that
   * node.
   * <p>
   * YARN uses the size and timestamp are used to check if the file has changed on HDFS
   * to check if YARN can use an existing copy, if any.
   * <p>
   * Resources are made public.
   *
   * @param conf         Configuration created from the Hadoop config files,
   *                     in this case, identifies the target file system.
   * @param resourcePath the path (relative or absolute) to the file on the
   *                     configured file system (usually HDFS).
   * @return a YARN local resource records that contains information about
   * path, size, type, resource and so on that YARN requires.
   * @throws IOException if the resource does not exist on the configured
   *                     file system
   */

  public LocalResource makeResource( Path dfsPath,
                                     LocalResourceType type,
                                     LocalResourceVisibility visibility ) throws DfsFacadeException {
    FileStatus dfsFileStatus;
    try {
      dfsFileStatus = fs.getFileStatus(dfsPath);
    } catch (IOException e) {
      throw new DfsFacadeException( "Failed to get DFS status for file: " + dfsPath, e );
    }
    URL destUrl;
    try {
      destUrl = ConverterUtils.getYarnUrlFromPath(
          FileContext.getFileContext( ).makeQualified(dfsPath));
    } catch (UnsupportedFileSystemException e) {
      throw new DfsFacadeException( "Unable to convert dfs file to a URL: " + dfsPath.toString(), e );
    }
    LocalResource resource = LocalResource.newInstance(
            destUrl,
            type, visibility,
            dfsFileStatus.getLen(), dfsFileStatus.getModificationTime());
    return resource;
  }

}
