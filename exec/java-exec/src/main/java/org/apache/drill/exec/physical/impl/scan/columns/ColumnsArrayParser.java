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
package org.apache.drill.exec.physical.impl.scan.columns;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsArrayManager.UnresolvedColumnsArrayColumn;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ColumnProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ScanProjectionParser;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.UnresolvedColumn;
import org.apache.drill.exec.vector.ValueVector;

public class ColumnsArrayParser implements ScanProjectionParser {

  // Internals

  private ScanLevelProjection builder;
  private List<Integer> columnsIndexes;
  private int maxIndex;

  // Output

  private UnresolvedColumnsArrayColumn columnsArrayCol;

  public ColumnsArrayParser() { }

  @Override
  public void bind(ScanLevelProjection builder) {
    this.builder = builder;
  }

  @Override
  public boolean parse(SchemaPath inCol) {
    if (! inCol.nameEquals(ColumnsArrayManager.COLUMNS_COL)) {
      return false;
    }

    // Special `columns` array column.

    mapColumnsArrayColumn(inCol);
    return true;
  }

  private void mapColumnsArrayColumn(SchemaPath inCol) {

    if (inCol.isArray()) {
      mapColumnsArrayElement(inCol);
      return;
    }

    // Query contains a reference to the "columns" generic
    // columns array. The query can refer to this column only once
    // (in non-indexed form.)

    if (columnsIndexes != null) {
      throw new IllegalArgumentException("Cannot refer to both columns and columns[i]");
    }
    if (columnsArrayCol != null) {
      throw new IllegalArgumentException("Duplicate columns[] column");
    }
    addColumnsArrayColumn(inCol);
  }

  private void addColumnsArrayColumn(SchemaPath inCol) {
    columnsArrayCol = new UnresolvedColumnsArrayColumn(inCol);
    builder.addProjectedColumn(columnsArrayCol);
  }

  private void mapColumnsArrayElement(SchemaPath inCol) {

    // Add the "columns" column, if not already present.
    // The project list past this point will contain just the
    // "columns" entry rather than the series of
    // columns[1], columns[2], etc. items that appear in the original
    // project list.

    if (columnsArrayCol == null) {
      addColumnsArrayColumn(SchemaPath.getSimplePath(inCol.rootName()));

      // Check if "columns" already appeared without an index.

    } else if (columnsIndexes == null) {
      throw new IllegalArgumentException("Cannot refer to both columns and columns[i]");
    }
    if (columnsIndexes == null) {
      columnsIndexes = new ArrayList<>();
    }
    int index = inCol.getRootSegment().getChild().getArraySegment().getIndex();
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
  public void validateColumn(ColumnProjection col) {
    if (columnsArrayCol != null && col.nodeType() == UnresolvedColumn.UNRESOLVED) {
      throw new IllegalArgumentException("Cannot select columns[] and other table columns: " + col.name());
    }
  }

  @Override
  public void build() {
    if (columnsIndexes == null) {
      return;
    }
    boolean indexes[] = new boolean[maxIndex + 1];
    for (Integer index : columnsIndexes) {
      indexes[index] = true;
    }
    columnsArrayCol.setIndexes(indexes);
  }

  public UnresolvedColumnsArrayColumn columnsArrayCol() { return columnsArrayCol; }
}