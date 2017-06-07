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
package org.apache.drill.exec.physical.impl.scan;

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
import org.apache.drill.exec.physical.impl.scan.SchemaNegotiator.ColumnType;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.store.ImplicitColumnExplorer.ImplicitFileColumns;
import org.apache.hadoop.fs.Path;

import jersey.repackaged.com.google.common.collect.Lists;

public class SelectionListPlan {

  public enum SelectType { WILDCARD, COLUMNS_ARRAY, LIST }

  public static final String WILDCARD = "*";
  public static final String COLUMNS_ARRAY_NAME = "columns";

  /**
   * Definition of a column from the SELECT list. This is
   * the column before semantic analysis is done to determine
   * the meaning of the column name.
   */

  public static class SelectColumn {
    private final String name;
    private final ColumnType typeHint;
    protected OutputColumn resolution;

    public SelectColumn(SchemaPath col) {
      this(col, null);
    }

    public SelectColumn(SchemaPath col, ColumnType hint) {
      this.name = col.getAsUnescapedPath();
      typeHint = hint;
    }

    public String name() { return name; }

    /**
     * Return the output column to which this input column is projected.
     *
     * @return the corresponding output column
     */

    public OutputColumn resolution() {
      return resolution;
    }

    /**
     * Return if this column is the special "*" column which means to select
     * all table columns.
     *
     * @return true if the column is "*"
     */

    public boolean isWildcard() { return name.equals(WILDCARD); }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder()
        .append("[InputColumn name=\"")
        .append(name)
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
  }

  /**
   * Specify the file name and optional selection root. If the selection root
   * is provided, then partitions are defined as the portion of the file name
   * that is not also part of the selection root. That is, if selection root is
   * /a/b and the file path is /a/b/c/d.csv, then dir0 is c.
   */

  public static class ResolvedFileInfo {

    private final Path filePath;
    private final String[] dirPath;

    public ResolvedFileInfo(Path filePath, String selectionRoot) {
      this.filePath = filePath;
      if (selectionRoot == null) {
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

    public Path filePath() { return filePath; }

    public String partition(int index) {
      if (dirPath == null ||  dirPath.length <= index) {
        return null;
      }
      return dirPath[index];
    }
  }

  /**
   * Represents a column in the output rows (record batch, result set) from
   * the scan operator. Each column has an index, which is the column's position
   * within the output tuple.
   */

  public abstract static class OutputColumn {
    public enum ColumnType { TABLE, FILE_METADATA, PARTITION, COLUMNS_ARRAY, WILDCARD }

    protected final SelectColumn inCol;

    public OutputColumn(SelectColumn inCol) {
      this.inCol = inCol;
    }

    public abstract ColumnType columnType();

    public String name() { return inCol.name(); }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder()
          .append("[")
          .append(getClass().getSimpleName());
      buildString(buf);
      buf.append("]");
      return buf.toString();
    }

    protected void buildString(StringBuilder buf) { }

    public SelectColumn source() { return inCol; }
  }

  public static class WildcardColumn extends OutputColumn {

    public final boolean includePartitions;

    public WildcardColumn(SelectColumn inCol, boolean includePartitions) {
      super(inCol);
      this.includePartitions = includePartitions;
    }

    @Override
    public ColumnType columnType() { return ColumnType.WILDCARD; }

    @Override
    protected void buildString(StringBuilder buf) {
      super.buildString(buf);
      buf.append(", partitions=")
         .append(includePartitions);
    }

    public boolean includePartitions() { return includePartitions; }
  }

  public static class ExpectedTableColumn extends OutputColumn {

    public ExpectedTableColumn(SelectColumn inCol) {
      super(inCol);
    }

    @Override
    public ColumnType columnType() { return ColumnType.TABLE; }
  }

  public static abstract class TypedColumn extends OutputColumn {

    private final MajorType type;

    public TypedColumn(SelectColumn inCol, MajorType type) {
      super(inCol);
      this.type = type;
    }

    public MajorType type() { return type; }

    @Override
    protected void buildString(StringBuilder buf) {
      super.buildString(buf);
      buf.append(", type=")
         .append(type.toString());
    }
  }

  /**
   * Represents Drill's special "columns" column which holds all actual columns
   * as an array of Varchars. There can be only one such column in the SELECT
   * list.
   */

  public static class ColumnsArrayColumn extends TypedColumn {

    public ColumnsArrayColumn(SelectColumn inCol) {
      super(inCol,
          MajorType.newBuilder()
          .setMinorType(MinorType.VARCHAR)
          .setMode(DataMode.REPEATED)
          .build());
    }

    @Override
    public ColumnType columnType() { return ColumnType.COLUMNS_ARRAY; }
  }

  /**
   * Base class for the various static (implicit) columns. Holds the
   * value of the column.
   */

  public abstract static class MetadataColumn extends TypedColumn {

    public MetadataColumn(SelectColumn inCol, MajorType type) {
      super(inCol, type);
    }

    public abstract String value(ResolvedFileInfo fileInfo);
  }

  /**
   * Represents an output column created from an implicit column. Since
   * values are known before reading data, the value is provided
   * along with the column definition.
   */

  public static class FileMetadataColumn extends MetadataColumn {

    private final FileMetadataColumnDefn defn;

    public FileMetadataColumn(SelectColumn inCol, FileMetadataColumnDefn defn) {
      super(inCol, MajorType.newBuilder()
            .setMinorType(MinorType.VARCHAR)
            .setMode(DataMode.REQUIRED)
            .build());
      this.defn = defn;
    }

    @Override
    public ColumnType columnType() { return ColumnType.FILE_METADATA; }

    @Override
    public String name() {
      if (inCol.isWildcard()) {
        return defn.colName;
      } else {
        return super.name();
      }
    }

    @Override
    public String value(ResolvedFileInfo fileInfo) {
      return defn.defn.getValue(fileInfo.filePath());
    }

    @Override
    protected void buildString(StringBuilder buf) {
      super.buildString(buf);
      buf.append(", defn=")
         .append(defn);
    }
  }

  /**
   * Partition output column for "dir<n>" for some n.
   * Data type is optional because some files may be more deeply
   * nested than others, so some files may have, say a dir2
   * while others do not.
   * <p>
   * The "dir" portion is customizable via a session option.
   * <p>
   * The value of the partition is known up front, and so the value
   * is stored in this column definition.
   */

  public static class PartitionColumn extends MetadataColumn {
    private final int partition;

    public PartitionColumn(SelectColumn inCol, int partition) {
      super(inCol, MajorType.newBuilder()
          .setMinorType(MinorType.VARCHAR)
          .setMode(DataMode.OPTIONAL)
          .build());
      this.partition = partition;
    }

    @Override
    public ColumnType columnType() { return ColumnType.PARTITION; }

    @Override
    public String value(ResolvedFileInfo fileInfo) {
      return fileInfo.partition(partition);
    }

    @Override
    protected void buildString(StringBuilder buf) {
      super.buildString(buf);
      buf.append(", partition=")
         .append(partition);
    }

    public int partition() { return partition; }
  }

  public static class Builder {

    // Config

    private final String partitionDesignator;
    private final Pattern partitionPattern;
    private List<FileMetadataColumnDefn> implicitColDefns = new ArrayList<>();;
    private Map<String, FileMetadataColumnDefn> fileMetadataColIndex = CaseInsensitiveMap.newHashMap();
    private boolean useLegacyStarPlan = true;

    // Input

    protected List<SelectColumn> queryCols = new ArrayList<>();

    // Output

    protected List<OutputColumn> outputCols = new ArrayList<>();
    protected ColumnsArrayColumn columnsArrayCol;
    protected SelectType selectType;
    protected SelectColumn starColumn;

    public Builder(OptionSet optionManager) {
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

    /**
     * Indicate a SELECT * query.
     *
     * @return this builder
     */
    public Builder selectAll() {
      return queryCols(Lists.newArrayList(new SchemaPath[] {SchemaPath.getSimplePath(WILDCARD)}));
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

    public Builder useLegacyStarPlan(boolean flag) {
      useLegacyStarPlan = flag;
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
    public Builder queryCols(List<SchemaPath> queryCols) {
      assert this.queryCols.isEmpty();
      this.queryCols = new ArrayList<>();
      for (SchemaPath col : queryCols) {
        SelectColumn sCol = new SelectColumn(col);
        this.queryCols.add(sCol);
      }
      return this;
    }

    public void selection(List<SelectColumn> queryCols) {
      assert this.queryCols.isEmpty();
      this.queryCols.addAll(queryCols);
    }

    /**
     * Perform projection planning and return the final projection plan.
     * @return the finalized projection plan
     */

    public SelectionListPlan build() {
      selectType = SelectType.LIST;
      for (SelectColumn inCol : queryCols) {
        mapColumn(inCol);
      }
      verify();
      return new SelectionListPlan(this);
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

    private void mapColumn(SelectColumn inCol) {
      if (inCol.isWildcard()) {

        // Star column: this is a SELECT * query.

        mapWildcardColumn(inCol);
        return;
      }
      Matcher m = partitionPattern.matcher(inCol.name());
      if (m.matches()) {

        // Partition column

        PartitionColumn outCol = new PartitionColumn(inCol, Integer.parseInt(m.group(1)));
        inCol.resolution = outCol;
        outputCols.add(outCol);
        return;
      }

      FileMetadataColumnDefn iCol = fileMetadataColIndex.get(inCol.name());
      if (iCol != null) {

        // File metadata (implicit) column

        FileMetadataColumn outCol = new FileMetadataColumn(inCol, iCol);
        inCol.resolution = outCol;
        outputCols.add(outCol);
        return;
      }

      if (inCol.name().equalsIgnoreCase(COLUMNS_ARRAY_NAME)) {

        // Special `columns` array column.

        mapColumnsArrayColumn(inCol);
        return;
      }

      // This is a desired table column.

      ExpectedTableColumn tableCol = new ExpectedTableColumn(inCol);
      inCol.resolution = tableCol;
      outputCols.add(tableCol);
    }

    private void mapWildcardColumn(SelectColumn inCol) {
      if (starColumn != null) {
        throw new IllegalArgumentException("Duplicate * entry in select list");
      }
      selectType = SelectType.WILDCARD;
      starColumn = inCol;

      // Put the wildcard column into the projection list as a placeholder to be filled
      // in later with actual table columns.

      inCol.resolution = new WildcardColumn(inCol, useLegacyStarPlan);
      outputCols.add(inCol.resolution);
      if (useLegacyStarPlan) {
        for (FileMetadataColumnDefn iCol : implicitColDefns) {
          FileMetadataColumn outCol = new FileMetadataColumn(starColumn, iCol);
          outputCols.add(outCol);
        }
      }
    }

    private void mapColumnsArrayColumn(SelectColumn inCol) {

      if (columnsArrayCol != null) {
        throw new IllegalArgumentException("Duplicate columns[] column");
      }

      selectType = SelectType.COLUMNS_ARRAY;
      columnsArrayCol = new ColumnsArrayColumn(inCol);
      outputCols.add(columnsArrayCol);
      inCol.resolution = columnsArrayCol;
    }

    private void verify() {
      if (starColumn != null && columnsArrayCol != null) {
        throw new IllegalArgumentException("Cannot select columns[] and `*` together");
      }
      for (OutputColumn outCol : outputCols) {
        if (outCol.columnType() == OutputColumn.ColumnType.TABLE) {
          if (starColumn != null) {
            throw new IllegalArgumentException("Cannot select table columns and `*` together");
          }
          if (columnsArrayCol != null) {
            throw new IllegalArgumentException("Cannot select columns[] and other table columns: " + outCol.name());
          }
        }
      }
    }
  }

  private final SelectType selectType;
  private final List<SelectColumn> queryCols;
  private final List<OutputColumn> outputCols;

  public SelectionListPlan(Builder builder) {
    selectType = builder.selectType;
    queryCols = builder.queryCols;
    outputCols = builder.outputCols;
  }

  /**
   * Return whether this is a SELECT * query
   * @return true if this is a SELECT * query
   */

  public boolean isSelectAll() { return selectType == SelectType.WILDCARD; }

  public SelectType selectType() { return selectType; }

  /**
   * Return the set of columns from the SELECT list
   * @return the SELECT list columns, in SELECT list order,
   * or null if this is a SELECT * query
   */

  public List<SelectColumn> queryCols() { return queryCols; }

  /**
   * The entire set of output columns, in output order. Output order is
   * that specified in the SELECT (for an explicit list of columns) or
   * table order (for SELECT * queries).
   * @return the set of output columns in output order
   */

  public List<OutputColumn> outputCols() { return outputCols; }

  // TODO: Test allowing * (for table) along with metadata columns
}
