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

import org.apache.drill.yarn.core.DrillOnYarnConfig;

import com.typesafe.config.Config;

import org.apache.drill.yarn.core.DoYUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CleanCommand extends ClientCommand {
  private Config config;
  private FileSystem fs;

  @Override
  public void run() throws ClientException {
    config = DrillOnYarnConfig.config();
    if (!isLocalized()) {
      System.out.println("Not using localized files; nothing to clean.");
      return;
    }
    fs = connectToFs( );
    removeDrillArchive();
    removeSiteArchive();
  }

  public boolean isLocalized() {
    return config.getBoolean(DrillOnYarnConfig.LOCALIZE_DRILL);
  }

  private void removeDrillArchive() {
    String localArchivePath = config
        .getString(DrillOnYarnConfig.DRILL_ARCHIVE_PATH);
    String archiveName = new File(localArchivePath).getName();
    removeArchive(archiveName);
  }

  private void removeArchive(String archiveName) {
    System.out.print("Removing " + archiveName + " ...");
    try {
      removeDrillFile(archiveName);
      System.out.println(" Removed");
      ;
    } catch (IOException e) {
      System.out.println();
      System.err.println(e.getMessage());
    }
  }

  public void removeDrillFile(String fileName) throws IOException {
    Path destPath = DoYUtil.getUploadPath(fs,fileName);
    try {
      fs.delete(destPath, false);
    } catch (IOException e) {
      throw new IOException(
          "Failed to delete file: " + destPath.toString(), e);
    }

    // Remove the Drill directory, but only if it is now empty.

    Path dir = destPath.getParent();
    try {
      FileStatus status[] = fs.listStatus( dir );
      if (status.length == 0) {
        fs.delete(dir, false);
      }
    } catch (IOException e) {
      throw new IOException(
          "Failed to delete directory: " + dir.toString(), e);
    }
  }
  
  private void removeSiteArchive() {
    DrillOnYarnConfig doyConfig = DrillOnYarnConfig.instance();
    if (!doyConfig.hasSiteDir()) {
      return;
    }
    String archiveName = DrillOnYarnConfig.SITE_ARCHIVE_NAME;
    removeArchive(archiveName);
  }

}
