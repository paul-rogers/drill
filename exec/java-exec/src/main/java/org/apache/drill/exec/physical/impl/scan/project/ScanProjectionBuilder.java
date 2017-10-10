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

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ProjectionType;
import org.apache.drill.exec.physical.impl.scan.project.ScanOutputColumn.RequestedTableColumn;
import org.apache.drill.exec.physical.impl.scan.project.ScanOutputColumn.WildcardColumn;

import com.google.common.collect.Lists;

/**
 * Fluent builder for the projection mapping. Accepts the inputs needed to
 * plan a projection, builds the mappings, and constructs the projection
 * mapping object.
 * <p>
 * Builds the per-scan projection plan given a set of projected columns.
 * Determines the output schema, which columns to project from the data
 * source, which are metadata, and so on.
 */

public class ScanProjectionBuilder {

  // Input

  protected List<SchemaPath> projectionList = new ArrayList<>();
  protected List<ScanProjectionParser> parsers = new ArrayList<>();

  // Output

  protected List<ScanOutputColumn> outputCols = new ArrayList<>();
  protected List<String> tableColNames = new ArrayList<>();
  protected ProjectionType projectionType;
  protected SchemaPath wildcardColumn;

  public ScanProjectionBuilder() {}

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
    return projectedCols(Lists.newArrayList(new SchemaPath[] {SchemaPath.getSimplePath(SchemaPath.WILDCARD)}));
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
    assert projectionList.isEmpty();
    projectionList.addAll(queryCols);
    return this;
  }

  public void requestColumns(List<SchemaPath> queryCols) {
    assert projectionList.isEmpty();
    projectionList.addAll(queryCols);
  }

  /**
   * Perform projection planning and return the final projection plan.
   * @return the finalized projection plan
   */

  public ScanLevelProjection build() {
    projectionType = ProjectionType.LIST;
    for (SchemaPath inCol : projectionList) {
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

  private void mapColumn(SchemaPath inCol) {

    // Give the extensions first crack at each column.
    // Some may want to "sniff" a column, even if they
    // don't fully handle it.

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

    // This is a desired table column.

    RequestedTableColumn tableCol = RequestedTableColumn.fromSelect(inCol);
    outputCols.add(tableCol);
    tableColNames.add(tableCol.name());
  }

  public void setProjectionType(ProjectionType type) {
    projectionType = type;
  }

  public void addProjectedColumn(ScanOutputColumn outCol) {
    outputCols.add(outCol);
  }

  private void mapWildcardColumn(SchemaPath inCol) {
    if (wildcardColumn != null) {
      throw new IllegalArgumentException("Duplicate * entry in project list");
    }
    projectionType = ProjectionType.WILDCARD;
    wildcardColumn = inCol;

    // Put the wildcard column into the projection list as a placeholder to be filled
    // in later with actual table columns.

    outputCols.add(WildcardColumn.fromSelect(inCol));
  }

  private void verify() {

    // Let parsers do overall validation.

    for (ScanProjectionParser parser : parsers) {
      parser.validate();
    }

    // Validate column-by-column.

    for (ScanOutputColumn outCol : outputCols) {
      for (ScanProjectionParser parser : parsers) {
        parser.validateColumn(outCol);
      }
      switch (outCol.columnType()) {
      case TABLE:
        if (hasWildcard()) {
          throw new IllegalArgumentException("Cannot select table columns and * together");
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