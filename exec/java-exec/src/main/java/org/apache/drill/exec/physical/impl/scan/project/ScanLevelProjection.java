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

import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.project.Exp.UnresolvedProjection;

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

public class ScanLevelProjection implements UnresolvedProjection {

  private final List<SchemaPath> requestedCols;
  private final List<ColumnProjection> outputCols;
  public final boolean hasWildcard;

  public ScanLevelProjection(ScanProjectionBuilder builder) {
    hasWildcard = builder.hasWildcard;
    requestedCols = builder.projectionList;
    outputCols = builder.outputCols;
  }

  /**
   * Return whether this is a SELECT * query
   * @return true if this is a SELECT * query
   */

  @Override
  public boolean projectAll() { return hasWildcard; }

  /**
   * Return the set of columns from the SELECT list
   * @return the SELECT list columns, in SELECT list order
   */

  public List<SchemaPath> requestedCols() { return requestedCols; }

  /**
   * The entire set of output columns, in output order. Output order is
   * that specified in the SELECT (for an explicit list of columns) or
   * table order (for SELECT * queries).
   * @return the set of output columns in output order
   */

  @Override
  public List<ColumnProjection> outputCols() { return outputCols; }

  @Override
  public MajorType nullType() {
    return MajorType.newBuilder()
        .setMinorType(MinorType.NULL)
        .setMode(DataMode.OPTIONAL)
        .build();
  }
}
