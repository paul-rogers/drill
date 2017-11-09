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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.PathSegment.ArraySegment;
import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.record.TupleNameSpace;

/**
 * Parses and analyzes the projection list passed to the scanner. The
 * projection list is per scan, independent of any tables that the
 * scanner might scan. The projection list is then used as input to the
 * per-table projection planning.
 * <p>
 * Accepts the inputs needed to
 * plan a projection, builds the mappings, and constructs the projection
 * mapping object.
 * <p>
 * Builds the per-scan projection plan given a set of projected columns.
 * Determines the output schema, which columns to project from the data
 * source, which are metadata, and so on.
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
 * <li<tt>SELECT ...</tt>: SELECT nothing, occurs in <tt>SELECT COUNT(*)</tt>
 * type queries. The provided projection list contains no (table) columns, though
 * it may contain metadata columns.</li>
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

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanLevelProjection.class);

  public static class NameElement {
    private final String name;
    private TupleNameSpace<NameElement> members;
    private Set<Integer> indexes;
    private NameElement parent;

    public NameElement(String name) {
      this.name = name;
    }

    public String name() { return name; }
    public boolean isWildcard() { return name.equals(SchemaPath.WILDCARD); }
    public boolean isSimple() { return ! isTuple() && ! isArray(); }
    public boolean isArray() { return indexes != null; }
    public boolean isTuple() { return members != null; }

    public void addMember(NameElement member) {
      if (members == null) {
        members = new TupleNameSpace<>();
      }
      members.add(member.name(), member);
      member.parent = this;
    }

    public NameElement member(String name) {
      return members == null ? null : members.get(name);
    }

    public List<NameElement> members() {
      return members == null ? null : members.entries();
    }

    public void addIndex(int index) {
      if (indexes == null) {
        indexes = new HashSet<>();
      }
      indexes.add(index);
    }

    public boolean hasIndex(int index) {
      return indexes == null ? false : indexes.contains(index);
    }

    public boolean[] indexes() {
      if (indexes == null) {
        return null;
      }
      int max = 0;
      for (Integer index : indexes) {
        max = Math.max(max, index);
      }
      boolean map[] = new boolean[max+1];
      for (Integer index : indexes) {
        map[index] = true;
      }
      return map;
    }

    public String fullName() {
      StringBuilder buf = new StringBuilder();
      buildName(buf);
      return buf.toString();
    }

    private void buildName(StringBuilder buf) {
      if (parent != null) {
        parent.buildName(buf);
      }
      buf.append('`')
         .append(name)
         .append('`');
    }
  }

  public static class ProjectionColumnParser {

    private TupleNameSpace<NameElement> cols = new TupleNameSpace<>();

    public List<NameElement> parse(List<SchemaPath> projection) {
      for (SchemaPath col : projection) {
        parse(col);
      }
      return cols.entries();
    }

    private void parse(SchemaPath col) {
      String name = col.rootName();
      NameElement element = cols.get(name);
      if (element != null) {
        if (col.isLeaf() && element.isSimple()) {
          throw UserException
            .validationError()
            .message("Duplicate column in project list: `%s`", name)
            .build(logger);
        }
      } else {
        element = new NameElement(name);
        cols.add(name, element);
      }
      if (! col.isLeaf()) {
        parse(element, col.getRootSegment().getChild());
      }
    }

    private void parse(NameElement element, PathSegment pathSeg) {
      if (pathSeg.isNamed()) {
        parseName(element, (NameSegment) pathSeg);
       } else {
        assert pathSeg.isArray();
        parseArray(element, (ArraySegment) pathSeg);
      }
    }

    private void parseName(NameElement element, NameSegment nameSeg) {
      String name = nameSeg.getPath();
      NameElement member = element.member(name);
      if (member != null) {
        if (nameSeg.isLastPath() && member.isSimple()) {
          throw UserException
            .validationError()
            .message("Duplicate column in projected map: %s.`%s`",
                element.fullName(), name)
            .build(logger);
        }
      } else {
        member = new NameElement(name);
        element.addMember(member);
      }
      if (! nameSeg.isLastPath()) {
        parse(member, nameSeg.getChild());
      }
    }

    private void parseArray(NameElement element, ArraySegment arraySeg) {
      int index = arraySeg.getIndex();
      if (element.hasIndex(index)) {
        throw UserException
          .validationError()
          .message("Duplicate array index in project list: %s[%d]",
              element.fullName(), index)
          .build(logger);
      }
      element.addIndex(index);
    }
  }

  public interface ColumnProjection {
    String name();
    int nodeType();
    boolean isTableProjection();
  }

  /**
   * Interface for add-on parsers, avoids the need to create
   * a single, tightly-coupled parser for all types of columns.
   * The main parser handles wildcards and assumes the rest of
   * the columns are table columns. The add-on parser can tag
   * columns as special, such as to hold metadata.
   */

  public interface ScanProjectionParser {
    void bind(ScanLevelProjection builder);
    boolean parse(SchemaPath inCol);
    void validate();
    void validateColumn(ColumnProjection col);
    void build();
  }

  public static class UnresolvedColumn implements ColumnProjection {

    public static final int WILDCARD = 1;
    public static final int UNRESOLVED = 2;

    /**
     * The original physical plan column to which this output column
     * maps. In some cases, multiple output columns map map the to the
     * same "input" (to the projection process) column.
     */

    private final SchemaPath inCol;
    private final int id;

    public UnresolvedColumn(SchemaPath inCol, int id) {
      this.inCol = inCol;
      this.id = id;
    }

    @Override
    public int nodeType() { return id; }

    @Override
    public String name() { return inCol.rootName(); }

    public SchemaPath source() { return inCol; }

    @Override
    public boolean isTableProjection() {
      return id == UNRESOLVED;
    }
  }

  // Input

  protected final List<SchemaPath> projectionList;

  // Configuration

  protected List<ScanProjectionParser> parsers;

  private final boolean v1_12MetadataLocation;

  // Output

  protected List<ColumnProjection> outputCols = new ArrayList<>();
  protected boolean sawWildcard;
  protected boolean hasWildcard;
  protected boolean emptyProjection = true;

  /**
   * Specify the set of columns in the SELECT list. Since the column list
   * comes from the query planner, assumes that the planner has checked
   * the list for syntax and uniqueness.
   *
   * @param queryCols list of columns in the SELECT list in SELECT list order
   * @return this builder
   */
  public ScanLevelProjection(List<SchemaPath> projectionList,
      List<ScanProjectionParser> parsers,
      boolean v1_12MetadataLocation) {
    this.projectionList = projectionList;
    this.parsers = parsers;
    this.v1_12MetadataLocation = v1_12MetadataLocation;
    for (ScanProjectionParser parser : parsers) {
      parser.bind(this);
    }
    for (SchemaPath inCol : projectionList) {
      if (inCol.isWildcard()) {
        mapWildcard(inCol);
      } else {
        mapColumn(inCol);
      }
    }
    verify();
    for (ScanProjectionParser parser : parsers) {
      parser.build();
    }
  }

  public ScanLevelProjection(List<SchemaPath> projectionList,
      List<ScanProjectionParser> parsers) {
    this(projectionList, parsers, false);
  }

  /**
   * Wildcard is special: add it, then let parsers add any custom
   * columns that are needed. The order is important: we want custom
   * columns to follow table columns.
   */

  private void mapWildcard(SchemaPath inCol) {

    // Wildcard column: this is a SELECT * query.

    if (sawWildcard) {
      throw new IllegalArgumentException("Duplicate * entry in project list");
    }

    // Remember the wildcard position, if we need to insert it.
    // Ensures that the main wildcard expansion occurs before add-on
    // columns.

    int wildcardPosn = outputCols.size();

    // Parsers can consume the wildcard. But, all parsers must
    // have visibility to the wildcard column.

    for (ScanProjectionParser parser : parsers) {
      if (parser.parse(inCol)) {
        wildcardPosn = -1;
      }
    }

    // Set this flag only after the parser checks.

    sawWildcard = true;

    // If not consumed, put the wildcard column into the projection list as a
    // placeholder to be filled in later with actual table columns.

    if (wildcardPosn != -1) {

      // Drill 1.1 - 1.11 and Drill 1.13 or later put metadata columns after
      // data columns. Drill 1.12 moved them before data columns. For testing
      // and compatibility, the client can request to use the Drill 1.12 position,
      // though the after-data position is the default.
      //
      // Note that the after-data location is much more convenient for the dirx
      // partition columns since these vary in number across scans within the same query.
      // By putting them at the end, the index of all other columns remains
      // constant. Drill 1.12 broke that behavior, but Drill 1.13 restored it.
      //
      // This option can be removed in Drill 1.14 after things settle down.

      UnresolvedColumn wildcardCol = new UnresolvedColumn(inCol, UnresolvedColumn.WILDCARD);
      if (v1_12MetadataLocation) {
        outputCols.add(wildcardCol);
      } else {
        outputCols.add(wildcardPosn, wildcardCol);
      }
      hasWildcard = true;
      emptyProjection = false;
    }
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

  private void mapColumn(SchemaPath inCol) {

    // Give the extensions first crack at each column.
    // Some may want to "sniff" a column, even if they
    // don't fully handle it.

    for (ScanProjectionParser parser : parsers) {
      if (parser.parse(inCol)) {
        return;
      }
    }

    // This is a desired table column.

    addTableColumn(
        new UnresolvedColumn(inCol, UnresolvedColumn.UNRESOLVED));
  }

  public void addTableColumn(ColumnProjection outCol) {
    outputCols.add(outCol);
    emptyProjection = false;
  }

  public void addMetadataColumn(ColumnProjection outCol) {
    outputCols.add(outCol);
  }

  private void verify() {

    // Let parsers do overall validation.

    for (ScanProjectionParser parser : parsers) {
      parser.validate();
    }

    // Validate column-by-column.

    for (ColumnProjection outCol : outputCols) {
      for (ScanProjectionParser parser : parsers) {
        parser.validateColumn(outCol);
      }
      switch (outCol.nodeType()) {
      case UnresolvedColumn.UNRESOLVED:
        if (hasWildcard()) {
          throw new IllegalArgumentException("Cannot select table columns and * together");
        }
        break;
      default:
        break;
      }
    }
  }
  /**
   * Return the set of columns from the SELECT list
   * @return the SELECT list columns, in SELECT list order
   */

  public List<SchemaPath> requestedCols() { return projectionList; }

  /**
   * The entire set of output columns, in output order. Output order is
   * that specified in the SELECT (for an explicit list of columns) or
   * table order (for SELECT * queries).
   * @return the set of output columns in output order
   */

  public List<ColumnProjection> columns() { return outputCols; }

  public boolean hasWildcard() { return hasWildcard; }

  /**
   * Return whether this is a SELECT * query
   * @return true if this is a SELECT * query
   */

  public boolean projectAll() { return hasWildcard; }

  /**
   * Returns true if the projection list is empty. This usually
   * indicates a <tt>SELECT COUNT(*)</tt> query (though the scan
   * operator does not have the context to know that an empty
   * list does, in fact, imply a count-only query...)
   *
   * @return true if no table columns are projected, false
   * if at least one column is projected (or the query contained
   * the wildcard)
   */

  public boolean projectNone() { return emptyProjection; }

}