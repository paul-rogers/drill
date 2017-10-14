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
package org.apache.drill.exec.physical.impl.scan.file;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.directory.api.util.Strings;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.impl.scan.project.ColumnProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanProjectionBuilder;
import org.apache.drill.exec.physical.impl.scan.project.ScanProjectionParser;
import org.apache.drill.exec.physical.impl.scan.project.UnresolvedColumn;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.store.ColumnExplorer.ImplicitFileColumns;
import org.apache.hadoop.fs.Path;

public class FileMetadataColumnsParser implements ScanProjectionParser {

  // Input

  Path scanRootDir;

  // Config

  final String partitionDesignator;
  private final Pattern partitionPattern;
  List<FileMetadataColumnDefn> implicitColDefns = new ArrayList<>();;
  private Map<String, FileMetadataColumnDefn> fileMetadataColIndex = CaseInsensitiveMap.newHashMap();
  boolean useLegacyWildcardExpansion = true;

  // Internal

  private ScanProjectionBuilder builder;

  // Output

  boolean hasMetadata;
  private FileMetadataProjection projection;

  public FileMetadataColumnsParser(OptionSet optionManager) {
    partitionDesignator = optionManager.getString(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL);
    partitionPattern = Pattern.compile(partitionDesignator + "(\\d+)", Pattern.CASE_INSENSITIVE);
    for (ImplicitFileColumns e : ImplicitFileColumns.values()) {
      String colName = optionManager.getString(e.optionName());
      if (! Strings.isEmpty(colName)) {
        FileMetadataColumnDefn defn = new FileMetadataColumnDefn(colName, e);
        implicitColDefns.add(defn);
        fileMetadataColIndex.put(defn.colName, defn);
      }
    }
  }

  /**
   * Specifies whether to plan based on the legacy meaning of "*". See
   * <a href="https://issues.apache.org/jira/browse/DRILL-5542">DRILL-5542</a>.
   * If true, then the star column <i>includes</i> implicit and partition
   * columns. If false, then star matches <i>only</i> table columns.
   * @param flag true to use the legacy plan, false to use the revised
   * semantics
   * @return this builder
   */

  public FileMetadataColumnsParser useLegacyWildcardExpansion(boolean flag) {
    useLegacyWildcardExpansion = flag;
    return this;
  }

  public FileMetadataColumnsParser setScanRootDir(Path rootDir) {
    scanRootDir = rootDir;
    return this;
  }

  @Override
  public void bind(ScanProjectionBuilder builder) {
    this.builder = builder;
  }

  @Override
  public boolean parse(SchemaPath inCol) {
    Matcher m = partitionPattern.matcher(inCol.rootName());
    if (m.matches()) {

      // Partition column

      UnresolvedColumn outCol = new UnresolvedPartitionColumn(inCol,
          Integer.parseInt(m.group(1)));
      builder.addProjectedColumn(outCol);
      hasMetadata = true;
      return true;
    }

    FileMetadataColumnDefn iCol = fileMetadataColIndex.get(inCol.rootName());
    if (iCol != null) {

      // File metadata (implicit) column

      UnresolvedColumn outCol = new UnresolvedFileMetadataColumn(inCol, iCol);
      builder.addProjectedColumn(outCol);
      hasMetadata = true;
      return true;
    }
    if (inCol.isWildcard() && useLegacyWildcardExpansion) {

      // Star column: this is a SELECT * query.

      // Old-style wildcard handling inserts all metadata columns in
      // the scanner, removes them in Project.
      // Fill in the file metadata columns. Can do here because the
      // set is constant across all files.

      hasMetadata = true;

      // Don't consider this a match.
    }
    return false;
  }

  @Override
  public void validate() { }

  @Override
  public void validateColumn(ColumnProjection outCol) {
    if (outCol.nodeType() == UnresolvedFileMetadataColumn.ID) {
      if (builder.hasWildcard()  &&  useLegacyWildcardExpansion) {
        throw new IllegalArgumentException("Cannot select file metadata columns and `*` together");
      }
    } else if (outCol.nodeType() == UnresolvedPartitionColumn.ID) {
      if (builder.hasWildcard()  &&  useLegacyWildcardExpansion) {
        throw new IllegalArgumentException("Cannot select partitions and `*` together");
      }
    }
  }

  @Override
  public void build() {
    projection = new FileMetadataProjection(this);
  }

  public FileMetadataProjection getProjection() {
    return projection;
  }
}