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
package org.apache.drill.exec.physical.impl.scan.project;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.directory.api.util.Strings;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.impl.scan.project.ScanOutputColumn.FileMetadataColumn;
import org.apache.drill.exec.physical.impl.scan.project.ScanOutputColumn.PartitionColumn;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.store.ColumnExplorer.ImplicitFileColumns;
import org.apache.hadoop.fs.Path;

public class FileMetadataColumnsParser implements ScanProjectionParser {

  // Default file metadata column names; primarily for testing.

  public static final String FILE_NAME_COL = "filename";
  public static final String FULLY_QUALIFIED_NAME_COL = "fqn";
  public static final String FILE_PATH_COL = "filepath";
  public static final String SUFFIX_COL = "suffix";
  public static final String PARTITION_COL = "dir";

  /**
   * Definition of a file metadata (AKA "implicit") column for this query.
   * Provides the static definition, along
   * with the name set for the implicit column in the session options for the query.
   */

  public static class FileMetadataColumnDefn {
    public final ImplicitFileColumns defn;
    public final String colName;

    public FileMetadataColumnDefn(String colName, ImplicitFileColumns defn) {
      this.colName = colName;
      this.defn = defn;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder()
        .append("[FileInfoColumnDefn name=\"")
        .append(colName)
        .append("\", defn=")
        .append(defn)
        .append("]");
      return buf.toString();
    }

    public String colName() { return colName; }

    public MajorType dataType() {
      return MajorType.newBuilder()
          .setMinorType(MinorType.VARCHAR)
          .setMode(DataMode.REQUIRED)
          .build();
    }
  }

  /**
   * Specify the file name and optional selection root. If the selection root
   * is provided, then partitions are defined as the portion of the file name
   * that is not also part of the selection root. That is, if selection root is
   * /a/b and the file path is /a/b/c/d.csv, then dir0 is c.
   */

  public static class FileMetadata {

    private final Path filePath;
    private final String[] dirPath;

    public FileMetadata(Path filePath, String selectionRoot) {
      this.filePath = filePath;

      // If the data source is not a file, no file metadata is available.

      if (selectionRoot == null || filePath == null) {
        dirPath = null;
        return;
      }

      // Result of splitting /x/y is ["", "x", "y"], so ignore first.

      String[] r = Path.getPathWithoutSchemeAndAuthority(new Path(selectionRoot)).toString().split("/");

      // Result of splitting "/x/y/z.csv" is ["", "x", "y", "z.csv"], so ignore first and last

      String[] p = Path.getPathWithoutSchemeAndAuthority(filePath).toString().split("/");

      if (p.length - 1 < r.length) {
        throw new IllegalArgumentException("Selection root of \"" + selectionRoot +
                                        "\" is shorter than file path of \"" + filePath.toString() + "\"");
      }
      for (int i = 1; i < r.length; i++) {
        if (! r[i].equals(p[i])) {
          throw new IllegalArgumentException("Selection root of \"" + selectionRoot +
              "\" is not a leading path of \"" + filePath.toString() + "\"");
        }
      }
      dirPath = ArrayUtils.subarray(p, r.length, p.length - 1);
    }

    public FileMetadata(Path fileName, Path rootDir) {
      this(fileName, rootDir == null ? null : rootDir.toString());
    }

    public Path filePath() { return filePath; }

    public String partition(int index) {
      if (dirPath == null ||  dirPath.length <= index) {
        return null;
      }
      return dirPath[index];
    }

    public int dirPathLength() {
      return dirPath == null ? 0 : dirPath.length;
    }

    public boolean isSet() { return filePath != null; }
  }

  public static class FileMetadataProjection {
    private final String partitionDesignator;
    private final boolean hasMetadata;
    private final boolean useLegacyWildcardExpansion;
    private final String scanRootDir;
    private final List<FileMetadataColumnsParser.FileMetadataColumnDefn> fileMetadataColDefns;

    public FileMetadataProjection(FileMetadataColumnsParser builder) {
      partitionDesignator = builder.partitionDesignator;
      fileMetadataColDefns = builder.implicitColDefns;
      hasMetadata = builder.hasMetadata;
      useLegacyWildcardExpansion = builder.useLegacyWildcardExpansion;
      scanRootDir = builder.scanRootDir;
    }

    public boolean hasMetadata() { return hasMetadata; }

    public boolean useLegacyWildcardPartition() { return useLegacyWildcardExpansion; }

    public List<FileMetadataColumnsParser.FileMetadataColumnDefn> fileMetadataColDefns() { return fileMetadataColDefns; }

    public String partitionName(int partition) {
      return partitionDesignator + partition;
    }

    public FileMetadataColumnsParser.FileMetadata fileMetadata(Path filePath) {
      return new FileMetadata(filePath, scanRootDir);
    }

    public static MajorType partitionColType() {
      return MajorType.newBuilder()
            .setMinorType(MinorType.VARCHAR)
            .setMode(DataMode.OPTIONAL)
            .build();
    }

    public FileLevelProjection resolve(ScanLevelProjection scanProj, Path filePath) {
      return resolve(scanProj, fileMetadata(filePath));
    }

    public FileLevelProjection resolve(ScanLevelProjection scanProj, FileMetadataColumnsParser.FileMetadata fileInfo) {
      return FileLevelProjection.fromResolution(scanProj, this, fileInfo);
    }
  }

  // Input

  private String scanRootDir;

  // Config

  private final String partitionDesignator;
  private final Pattern partitionPattern;
  private List<FileMetadataColumnsParser.FileMetadataColumnDefn> implicitColDefns = new ArrayList<>();;
  private Map<String, FileMetadataColumnsParser.FileMetadataColumnDefn> fileMetadataColIndex = CaseInsensitiveMap.newHashMap();
  private boolean useLegacyWildcardExpansion = true;

  // Internal

  private ScanProjectionBuilder builder;

  // Output

  private boolean hasMetadata;
  private FileMetadataColumnsParser.FileMetadataProjection projection;

  public FileMetadataColumnsParser(OptionSet optionManager) {
    partitionDesignator = optionManager.getString(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL);
    partitionPattern = Pattern.compile(partitionDesignator + "(\\d+)", Pattern.CASE_INSENSITIVE);
    for (ImplicitFileColumns e : ImplicitFileColumns.values()) {
      String colName = optionManager.getString(e.optionName());
      if (! Strings.isEmpty(colName)) {
        FileMetadataColumnsParser.FileMetadataColumnDefn defn = new FileMetadataColumnDefn(colName, e);
        implicitColDefns.add(defn);
        fileMetadataColIndex.put(defn.colName, defn);
      }
    }
  }

  public static String partitionColName(int partition) {
    return PARTITION_COL + partition;
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

  public FileMetadataColumnsParser setScanRootDir(String rootDir) {
    this.scanRootDir = rootDir;
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

      PartitionColumn outCol = PartitionColumn.fromSelect(inCol, Integer.parseInt(m.group(1)));
      builder.addProjectedColumn(outCol);
      hasMetadata = true;
      return true;
    }

    FileMetadataColumnsParser.FileMetadataColumnDefn iCol = fileMetadataColIndex.get(inCol.rootName());
    if (iCol != null) {

      // File metadata (implicit) column

      FileMetadataColumn outCol = FileMetadataColumn.fromSelect(inCol, iCol);
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
  public void validateColumn(ScanOutputColumn outCol) {
    switch (outCol.columnType()) {
    case FILE_METADATA:
      if (builder.hasWildcard()  &&  useLegacyWildcardExpansion) {
        throw new IllegalArgumentException("Cannot select file metadata columns and `*` together");
      }
      break;
    case PARTITION:
      if (builder.hasWildcard()  &&  useLegacyWildcardExpansion) {
        throw new IllegalArgumentException("Cannot select partitions and `*` together");
      }
      break;
    default:
      break;
    }
  }

  @Override
  public void build() {
    projection = new FileMetadataProjection(this);
  }

  public FileMetadataColumnsParser.FileMetadataProjection getProjection() {
    return projection;
  }
}