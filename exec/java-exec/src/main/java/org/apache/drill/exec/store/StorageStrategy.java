/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;

/** Contains list of parameters that will be used to store files on file system. */
public class StorageStrategy {

  /**
   * Primary is used for persistent tables,
   * where owner and group have full access and others can not write,
   * tables files are not deleted on file system close.
   */
  public static final StorageStrategy PERSISTENT = new StorageStrategy("775", false);

  /**
   * Primary is used for temporary tables to guarantee
   * that only user who created folder and files has full access,
   * table files are deleted on file system close.
   */
  public static final StorageStrategy TEMPORARY = new StorageStrategy("700", true);

  private final String permission;
  private final boolean deleteOnExit;

  @JsonCreator
  public StorageStrategy(@JsonProperty("permission") String permission,
                         @JsonProperty("deleteOnExit") boolean deleteOnExit) {
    this.permission = permission;
    this.deleteOnExit = deleteOnExit;
  }

  public String getPermission() {
    return permission;
  }

  public boolean isDeleteOnExit() {
    return deleteOnExit;
  }

  /**
   * Applies storage strategy to passed path on passed file system.
   * Sets appropriate permission
   * and adds to file system delete on exit if needed.
   *
   * @param fs file system where path is located
   * @param path folder or file location
   * @throws IOException is thrown in case of problems while setting permission
   *         or adding path to delete on exit list
   */
  @JsonIgnore
  public void apply(FileSystem fs, Path path) throws IOException {
    fs.setPermission(path, new FsPermission(permission));
    if (deleteOnExit) {
      fs.deleteOnExit(path);
    }
  }

}