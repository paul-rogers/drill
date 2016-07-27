package org.apache.drill.yarn.core;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class Localizer {
  private final FileSystem fs;
  protected File localArchivePath;
  protected Path dfsArchivePath;
  FileStatus fileStatus;
  private String label;

  /**
   * Resources to be localized (downloaded) to each AM or drillbit node.
   */

  public Localizer(FileSystem fs, File archivePath, String label) {
    this(fs, archivePath, DoYUtil.getUploadPath(fs,archivePath), label);
  }

  public Localizer(FileSystem fs, File archivePath, String destName,
      String label) {
    this(fs, archivePath, DoYUtil.getUploadPath(fs,destName), label);
  }

  public Localizer(FileSystem fs, String destPath) {
    this( fs, null, new Path(destPath), null );
  }

  public Localizer(FileSystem fs, File archivePath, Path destPath, String label) {
    this.fs = fs;
    dfsArchivePath = destPath;
    this.label = label;
    localArchivePath = archivePath;
  }

  public String getBaseName() {
    return localArchivePath.getName();
  }

  public String getDestPath() {
    return dfsArchivePath.toString();
  }

  public void upload() throws IOException {
    uploadArchive(localArchivePath, dfsArchivePath, label);
    fileStatus = null;
  }

  public void uploadArchive(File localArchiveFile, Path destPath, String label)
      throws IOException {
    // Create the application upload directory if it does not yet exist.

    String dfsDirStr = DrillOnYarnConfig.config( ).getString(DrillOnYarnConfig.DFS_APP_DIR);
    Path appDir = new Path(dfsDirStr);
    try {
      // If the directory does not exist, create it, giving this user
      // (only) read and write access.

      if (!fs.isDirectory(appDir)) {
        fs.mkdirs(appDir, new FsPermission(FsAction.READ_WRITE, null, null));
      }
    } catch (IOException e) {
      throw new IOException(
          "Failed to create DFS directory: " + dfsDirStr, e);
    }

    // The file must be an archive type so YARN knows to extract its contents.

    String baseName = localArchiveFile.getName();
    if (DrillOnYarnConfig.findSuffix(baseName) == null) {
      throw new IOException(
          label + " archive must be .tar.gz, .tgz or .zip: " + baseName);
    }

    Path srcPath = new Path(localArchiveFile.getAbsolutePath());

    // Do the upload, replacing the old archive.

    try {
      // TODO: Specify file permissions and owner.

      fs.copyFromLocalFile(false, true, srcPath, destPath);
    } catch (IOException e) {
      throw new IOException(
          "Failed to upload " + label + " archive to DFS: "
              + localArchiveFile.getAbsolutePath() + " --> " + destPath,
          e);
    }
  }

  /**
   * The client may check file status multiple times. Cache it here so we
   * only retrieve the status once. Cache it here so that the client
   * doen't have to do the caching.
   *
   * @return
   * @throws DfsFacadeException
   */

  private FileStatus getStatus() throws IOException {
    if (fileStatus == null) {
      fileStatus = getFileStatus(dfsArchivePath);
    }
    return fileStatus;
  }

  private FileStatus getFileStatus(Path dfsPath) throws IOException {
    try {
      return fs.getFileStatus(dfsPath);
    } catch (IOException e) {
      throw new IOException(
          "Failed to get DFS status for file: " + dfsPath, e);
    }
  }

  public void defineResources(Map<String, LocalResource> resources,
      String key) throws IOException {
    // Put the application archive, visible to only the application.
    // Because it is an archive, it will be expanded by YARN prior to launch
    // of the AM.

    LocalResource drillResource = makeResource(dfsArchivePath,
        getStatus(), LocalResourceType.ARCHIVE,
        LocalResourceVisibility.APPLICATION);
    resources.put(key, drillResource);
  }

  /**
   * Create a local resource definition for YARN. A local resource is one that
   * must be localized onto the remote node prior to running a command on that
   * node.
   * <p>
   * YARN uses the size and timestamp are used to check if the file has changed
   * on HDFS to check if YARN can use an existing copy, if any.
   * <p>
   * Resources are made public.
   *
   * @param conf
   *          Configuration created from the Hadoop config files, in this case,
   *          identifies the target file system.
   * @param resourcePath
   *          the path (relative or absolute) to the file on the configured file
   *          system (usually HDFS).
   * @return a YARN local resource records that contains information about path,
   *         size, type, resource and so on that YARN requires.
   * @throws IOException
   *           if the resource does not exist on the configured file system
   */

  private LocalResource makeResource(Path dfsPath, FileStatus dfsFileStatus,
      LocalResourceType type, LocalResourceVisibility visibility)
      throws IOException {
    URL destUrl;
    try {
      destUrl = ConverterUtils.getYarnUrlFromPath(
          FileContext.getFileContext().makeQualified(dfsPath));
    } catch (UnsupportedFileSystemException e) {
      throw new IOException(
          "Unable to convert dfs file to a URL: " + dfsPath.toString(), e);
    }
    LocalResource resource = LocalResource.newInstance(destUrl, type,
        visibility, dfsFileStatus.getLen(),
        dfsFileStatus.getModificationTime());
    return resource;
  }
  
  public boolean filesMatch() {
    FileStatus status;
    try {
      status = getStatus();
    } catch (IOException e) {

      // An exception is DFS's way of tell us the file does
      // not exist.

      return false;
    }
    return status.getLen() == localArchivePath.length();
  }

  public String getLabel() {
    return label;
  }

  public boolean destExists() throws IOException {
    return fs.exists(dfsArchivePath);
  }
}