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
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.impl.scan.FileLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanOutputColumn.ColumnType;
import org.apache.drill.exec.physical.impl.scan.project.ScanOutputColumn.ColumnsArrayColumn;
import org.apache.drill.exec.physical.impl.scan.project.ScanOutputColumn.FileMetadataColumn;
import org.apache.drill.exec.physical.impl.scan.project.ScanOutputColumn.PartitionColumn;
import org.apache.drill.exec.physical.impl.scan.project.ScanOutputColumn.RequestedTableColumn;
import org.apache.drill.exec.physical.impl.scan.project.ScanOutputColumn.WildcardColumn;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.store.ColumnExplorer.ImplicitFileColumns;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

/**
 * Parses and analyzes the projection list passed to the scanner. The
 * projection list is per scan, independent of any tables that the
 * scanner might scan. The projection list is then used as input to the
 * per-table projection planning.
 * <p>
 * An annoying aspect of SQL is that the projection list (the list of
 * columns to appear in the output) is specified after the SELECT keyword.
 * In Relational theory, projection is about columns, selection is about
 * rows...
 * <p>
 * Mappings can be based on three primary use cases:
 * <ul>
 * <li><tt>SELECT *</tt>: Project all data source columns, whatever they happen
 * to be. Create columns using names from the data source. The data source
 * also determines the order of columns within the row.</li>
 * <li><tt>SELECT columns</tt>: Similar to SELECT * in that it projects all columns
 * from the data source, in data source order. But, rather than creating
 * individual output columns for each data source column, creates a single
 * column which is an array of Varchars which holds the (text form) of
 * each column as an array element.</li>
 * <li><tt>SELECT a, b, c, ...</tt>: Project a specific set of columns, identified by
 * case-insensitive name. The output row uses the names from the SELECT list,
 * but types from the data source. Columns appear in the row in the order
 * specified by the SELECT.</li>
 * </ul>
 * Names in the SELECT list can reference any of five distinct types of output
 * columns:
 * <ul>
 * <li>Wildcard ("*") column: indicates the place in the projection list to insert
 * the table columns once found in the table projection plan.</li>
 * <li>Data source columns: columns from the underlying table. The table
 * projection planner will determine if the column exists, or must be filled
 * in with a null column.</li>
 * <li>The generic data source columns array: <tt>columns</tt>, or optionally
 * specific members of the <tt>columns</tt> array such as <tt>columns[1]</tt>.</li>
 * <li>Implicit columns: <tt>fqn</tt>, <tt>filename</tt>, <tt>filepath</tt>
 * and <tt>suffix</tt>. These reference
 * parts of the name of the file being scanned.</li>
 * <li>Partition columns: <tt>dir0</tt>, <tt>dir1</tt>, ...: These reference
 * parts of the path name of the file.</li>
 * </ul>
 *
 * @see {@link ImplicitColumnExplorer}, the class from which this class
 * evolved
 */

public class ScanLevelProjection {

  public enum ProjectionType { WILDCARD, COLUMNS_ARRAY, LIST }

  public static final String WILDCARD = "*";
  public static final String COLUMNS_ARRAY_NAME = "columns";

  /**
   * Definition of a column from the SELECT list. This is
   * the column before semantic analysis is done to determine
   * the meaning of the column name.
   */

  public static class RequestedColumn {
    private final SchemaPath column;
    protected ScanOutputColumn resolution;

    public RequestedColumn(SchemaPath col) {
      column = col;
    }

    /**
     * Returns the root name:
     * <ul>
     * <li>a: returns a</li>
     * <li>a.b: returns a</li>
     * <li>a[10]: returns a</li>
     * </ul>
     * @return root name
     */

    public String name() { return column.getRootSegment().getPath(); }

    /**
     * Return the output column to which this input column is projected.
     *
     * @return the corresponding output column
     */

    public ScanOutputColumn resolution() {
      return resolution;
    }

    /**
     * Return if this column is the special wildcard ("*") column which means to
     * project all table columns.
     *
     * @return true if the column is "*"
     */

    public boolean isWildcard() { return name().equals(WILDCARD); }

    public boolean isColumnsArray() {
      return name().equalsIgnoreCase(COLUMNS_ARRAY_NAME);
    }

    public boolean isArray() {
      return ! column.isSimplePath();
    }

    public int arrayIndex() {
      return column.getRootSegment().getChild().getArraySegment().getIndex();
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder()
        .append("[InputColumn path=\"")
        .append(column)
        .append("\", resolution=");
      if (resolution == null) {
        buf.append("null");
      } else {
        buf.append(resolution.toString());
      }
      buf.append("]");
      return buf.toString();
    }
  }

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

  public static class NullColumnProjection extends ScanOutputColumn {

    public NullColumnProjection(RequestedColumn inCol) {
      super(inCol);
    }

    @Override
    public ColumnType columnType() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    protected void visit(int index, Visitor visitor) {
      // TODO Auto-generated method stub

    }
  }

  public interface ScanProjectionParser {
    void bind(ScanProjectionBuilder builder);
    boolean parse(RequestedColumn inCol);
    void validate();
    void validateColumn(ScanOutputColumn col);
    void build();
  }

  public static class ColumnsArrayParser implements ScanProjectionParser {

    // Config

    private MajorType columnsArrayType;

    // Internals

    private ScanProjectionBuilder builder;

    // Output

    protected ColumnsArrayColumn columnsArrayCol;
    protected List<Integer> columnsIndexes;
    protected int maxIndex;

    private ColumnsArrayProjection projection;

    public void columnsArrayType(MinorType type) {
      columnsArrayType = MajorType.newBuilder()
          .setMinorType(type)
          .setMode(DataMode.REPEATED)
          .build();
    }

    @Override
    public void bind(ScanProjectionBuilder builder) {
      this.builder = builder;
    }

    @Override
    public boolean parse(RequestedColumn inCol) {
      if (! inCol.name().equalsIgnoreCase(COLUMNS_ARRAY_NAME)) {
        return false;
      }

      // Special `columns` array column.

      mapColumnsArrayColumn(inCol);
      return true;
    }

    private void mapColumnsArrayColumn(RequestedColumn inCol) {

      if (inCol.isArray()) {
        mapColumnsArrayElement(inCol);
        return;
      }

      // Query contains a reference to the "columns" generic
      // columns array. The query can refer to this column only once
      // (in non-indexed form.)

      if (columnsArrayCol != null) {
        throw new IllegalArgumentException("Duplicate columns[] column");
      }
      if (columnsIndexes != null) {
        throw new IllegalArgumentException("Cannot refer to both columns and columns[i]");
      }
      addColumnsArrayColumn(inCol);
    }

    private void addColumnsArrayColumn(RequestedColumn inCol) {
      builder.setProjectionType(ProjectionType.COLUMNS_ARRAY);
      columnsArrayCol = ColumnsArrayColumn.fromSelect(inCol, columnsArrayType());
      inCol.resolution = columnsArrayCol;
      builder.addProjectedColumn(columnsArrayCol);
    }

    public MajorType columnsArrayType() {
      if (columnsArrayType == null) {
        columnsArrayType = MajorType.newBuilder()
            .setMinorType(MinorType.VARCHAR)
            .setMode(DataMode.REPEATED)
            .build();
      }
      return columnsArrayType;
    }

    private void mapColumnsArrayElement(RequestedColumn inCol) {
      // Add the "columns" column, if not already present.
      // The project list past this point will contain just the
      // "columns" entry rather than the series of
      // columns[1], columns[2], etc. items that appear in the original
      // project list.

      if (columnsArrayCol == null) {

        // Check if "columns" already appeared without an index.

        if (columnsIndexes == null) {
          throw new IllegalArgumentException("Cannot refer to both columns and columns[i]");
        }
        addColumnsArrayColumn(inCol);
        columnsIndexes = new ArrayList<>();
      }
      int index = inCol.arrayIndex();
      if (index < 0  ||  index > ValueVector.MAX_ROW_COUNT) {
        throw new IllegalArgumentException("columns[" + index + "] out of bounds");
      }
      columnsIndexes.add(index);
      maxIndex = Math.max(maxIndex, index);
    }

    @Override
    public void validate() {
      if (builder.hasWildcard() && columnsArrayCol != null) {
        throw new IllegalArgumentException("Cannot select columns[] and `*` together");
      }
    }

    @Override
    public void validateColumn(ScanOutputColumn col) {
      if (columnsArrayCol != null && col.columnType() == ColumnType.TABLE) {
        throw new IllegalArgumentException("Cannot select columns[] and other table columns: " + col.name());
      }
    }

    public static class ColumnsArrayProjection {
      private final boolean columnsIndexes[];
      private final MajorType columnsArrayType;

      public ColumnsArrayProjection(ColumnsArrayParser builder) {
        columnsArrayType = builder.columnsArrayType;
        if (builder.columnsIndexes == null) {
          columnsIndexes = null;
        } else {
          columnsIndexes = new boolean[builder.maxIndex];
          for (int i = 0; i < builder.columnsIndexes.size(); i++) {
            columnsIndexes[builder.columnsIndexes.get(i)] = true;
          }
        }
      }

      public boolean[] columnsArrayIndexes() { return columnsIndexes; }

      public MajorType columnsArrayType() { return columnsArrayType; }
    }

    @Override
    public void build() {
      projection = new ColumnsArrayProjection(this);
    }
  }

  /**
   * Fluent builder for the projection mapping. Accepts the inputs needed to
   * plan a projection, builds the mappings, and constructs the projection
   * mapping object.
   * <p>
   * Builds the per-scan projection plan given a set of projected columns.
   * Determines the output schema, which columns to project from the data
   * source, which are metadata, and so on.
   */

  public static class ScanProjectionBuilder {

    // Config

    private final String partitionDesignator;
    private final Pattern partitionPattern;
    private List<FileMetadataColumnDefn> implicitColDefns = new ArrayList<>();;
    private Map<String, FileMetadataColumnDefn> fileMetadataColIndex = CaseInsensitiveMap.newHashMap();
    private boolean useLegacyWildcardExpansion = true;

    // Input

    protected String scanRootDir;
    protected List<RequestedColumn> projectionList = new ArrayList<>();
    protected List<ScanProjectionParser> parsers = new ArrayList<>();

    // Output

    protected List<ScanOutputColumn> outputCols = new ArrayList<>();
    protected List<String> tableColNames = new ArrayList<>();
    protected ProjectionType projectionType;
    protected RequestedColumn wildcardColumn;
    protected boolean hasMetadata;

    public ScanProjectionBuilder(OptionSet optionManager) {
      partitionDesignator = optionManager.getOption(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL_VALIDATOR);
      partitionPattern = Pattern.compile(partitionDesignator + "(\\d+)", Pattern.CASE_INSENSITIVE);
      for (ImplicitFileColumns e : ImplicitFileColumns.values()) {
        OptionValue optionValue = optionManager.getOption(e.optionName());
        if (optionValue != null) {
          FileMetadataColumnDefn defn = new FileMetadataColumnDefn(optionValue.string_val, e);
          implicitColDefns.add(defn);
          fileMetadataColIndex.put(defn.colName, defn);
        }
      }
    }

    public void addParser(ScanProjectionParser parser) {
      parsers.add(parser);
      parser.bind(this);
    }

    /**
     * Indicate a SELECT * query.
     *
     * @return this builder
     */
    public ScanProjectionBuilder projectAll() {
      return projectedCols(Lists.newArrayList(new SchemaPath[] {SchemaPath.getSimplePath(WILDCARD)}));
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

    public ScanProjectionBuilder useLegacyWildcardExpansion(boolean flag) {
      useLegacyWildcardExpansion = flag;
      return this;
    }

    /**
     * Specify the set of columns in the SELECT list. Since the column list
     * comes from the query planner, assumes that the planner has checked
     * the list for syntax and uniqueness.
     *
     * @param queryCols list of columns in the SELECT list in SELECT list order
     * @return this builder
     */
    public ScanProjectionBuilder projectedCols(List<SchemaPath> queryCols) {
      assert this.projectionList.isEmpty();
      this.projectionList = new ArrayList<>();
      for (SchemaPath col : queryCols) {
        RequestedColumn sCol = new RequestedColumn(col);
        this.projectionList.add(sCol);
      }
      return this;
    }

    public void requestColumns(List<RequestedColumn> queryCols) {
      assert this.projectionList.isEmpty();
      this.projectionList.addAll(queryCols);
    }

    public void setScanRootDir(String rootDir) {
      this.scanRootDir = rootDir;
    }

    /**
     * Perform projection planning and return the final projection plan.
     * @return the finalized projection plan
     */

    public ScanLevelProjection build() {
      projectionType = ProjectionType.LIST;
      for (RequestedColumn inCol : projectionList) {
        mapColumn(inCol);
      }
      verify();
      for (ScanProjectionParser parser : parsers) {
        parser.build();
      }
      return new ScanLevelProjection(this);
    }

    /**
     * Map the column into one of five categories.
     * <ol>
     * <li>Star column (to designate SELECT *)</li>
     * <li>Partition file column (dir0, dir1, etc.)</li>
     * <li>Implicit column (fqn, filepath, filename, suffix)</li>
     * <li>Special <tt>columns</tt> column which holds all columns as
     * an array.</li>
     * <li>Table column. The actual match against the table schema
     * is done later.</li>
     * </ol>
     *
     * @param inCol the SELECT column
     */

    private void mapColumn(RequestedColumn inCol) {
      for (ScanProjectionParser parser : parsers) {
        if (parser.parse(inCol)) {
          return;
        }
      }
      if (inCol.isWildcard()) {

        // Star column: this is a SELECT * query.

        mapWildcardColumn(inCol);
        return;
      }
      Matcher m = partitionPattern.matcher(inCol.name());
      if (m.matches()) {

        // Partition column

        PartitionColumn outCol = PartitionColumn.fromSelect(inCol, Integer.parseInt(m.group(1)));
        inCol.resolution = outCol;
        outputCols.add(outCol);
        hasMetadata = true;
        return;
      }

      FileMetadataColumnDefn iCol = fileMetadataColIndex.get(inCol.name());
      if (iCol != null) {

        // File metadata (implicit) column

        FileMetadataColumn outCol = FileMetadataColumn.fromSelect(inCol, iCol);
        inCol.resolution = outCol;
        outputCols.add(outCol);
        hasMetadata = true;
        return;
      }

      // This is a desired table column.

      RequestedTableColumn tableCol = RequestedTableColumn.fromSelect(inCol);
      inCol.resolution = tableCol;
      outputCols.add(tableCol);
      tableColNames.add(tableCol.name());
    }

    public void setProjectionType(ProjectionType type) {
      projectionType = type;
    }

    public void addProjectedColumn(ScanOutputColumn outCol) {
      outputCols.add(outCol);
    }

    private void mapWildcardColumn(RequestedColumn inCol) {
      if (wildcardColumn != null) {
        throw new IllegalArgumentException("Duplicate * entry in project list");
      }
      projectionType = ProjectionType.WILDCARD;
      wildcardColumn = inCol;

      // Put the wildcard column into the projection list as a placeholder to be filled
      // in later with actual table columns.

      inCol.resolution = WildcardColumn.fromSelect(inCol);
      if (useLegacyWildcardExpansion) {

        // Old-style wildcard handling inserts all metadata coluns in
        // the scanner, removes them in Project.
        // Fill in the file metadata columns. Can do here because the
        // set is constant across all files.

        hasMetadata = true;
      }
      outputCols.add(inCol.resolution);
    }

    private void verify() {
      for (ScanProjectionParser parser : parsers) {
        parser.validate();
      }
      for (ScanOutputColumn outCol : outputCols) {
        for (ScanProjectionParser parser : parsers) {
          parser.validateColumn(outCol);
        }
        switch (outCol.columnType()) {
        case TABLE:
          if (wildcardColumn != null) {
            throw new IllegalArgumentException("Cannot select table columns and `*` together");
          }
          break;
        case FILE_METADATA:
          if (wildcardColumn != null  &&  useLegacyWildcardExpansion) {
            throw new IllegalArgumentException("Cannot select file metadata columns and `*` together");
          }
          break;
        case PARTITION:
          if (wildcardColumn != null  &&  useLegacyWildcardExpansion) {
            throw new IllegalArgumentException("Cannot select partitions and `*` together");
          }
          break;
        default:
          break;
        }
      }
    }

    public boolean hasWildcard() {
      return wildcardColumn != null;
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

  private final String partitionDesignator;
  private final List<FileMetadataColumnDefn> fileMetadataColDefns;
  private final ProjectionType projectType;
  private final boolean hasMetadata;
  private final boolean useLegacyWildcardExpansion;
  private final String scanRootDir;
  private final List<RequestedColumn> requestedCols;
  private final List<ScanOutputColumn> outputCols;
  private final List<String> tableColNames;

  public ScanLevelProjection(ScanProjectionBuilder builder) {
    partitionDesignator = builder.partitionDesignator;
    fileMetadataColDefns = builder.implicitColDefns;
    projectType = builder.projectionType;
    hasMetadata = builder.hasMetadata;
    useLegacyWildcardExpansion = builder.useLegacyWildcardExpansion;
    scanRootDir = builder.scanRootDir;
    requestedCols = builder.projectionList;
    outputCols = builder.outputCols;
    tableColNames = builder.tableColNames;
  }

  /**
   * Return whether this is a SELECT * query
   * @return true if this is a SELECT * query
   */

  public boolean isProjectAll() { return projectType == ProjectionType.WILDCARD; }

  public ProjectionType projectType() { return projectType; }

  /**
   * Return the set of columns from the SELECT list
   * @return the SELECT list columns, in SELECT list order
   */

  public List<RequestedColumn> requestedCols() { return requestedCols; }

  public boolean hasMetadata() { return hasMetadata; }

  /**
   * The entire set of output columns, in output order. Output order is
   * that specified in the SELECT (for an explicit list of columns) or
   * table order (for SELECT * queries).
   * @return the set of output columns in output order
   */

  public List<ScanOutputColumn> outputCols() { return outputCols; }

  public boolean useLegacyWildcardPartition() { return useLegacyWildcardExpansion; }

  public List<String> tableColNames() { return tableColNames; }

  public List<FileMetadataColumnDefn> fileMetadataColDefns() { return fileMetadataColDefns; }

  public String partitionName(int partition) {
    return partitionDesignator + partition;
  }

  public FileMetadata fileMetadata(Path filePath) {
    return new FileMetadata(filePath, scanRootDir);
  }

  public static MajorType partitionColType() {
    return MajorType.newBuilder()
          .setMinorType(MinorType.VARCHAR)
          .setMode(DataMode.OPTIONAL)
          .build();
  }

  public static MajorType nullType() {
    return MajorType.newBuilder()
        .setMinorType(MinorType.NULL)
        .setMode(DataMode.OPTIONAL)
        .build();
  }

  public FileLevelProjection resolve(Path filePath) {
    return resolve(fileMetadata(filePath));
  }

  public FileLevelProjection resolve(FileMetadata fileInfo) {
    return FileLevelProjection.fromResolution(this, fileInfo);
  }
}
