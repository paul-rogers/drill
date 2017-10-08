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

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ProjectionType;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.RequestedColumn;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ScanProjectionBuilder;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ScanProjectionParser;
import org.apache.drill.exec.physical.impl.scan.project.ScanOutputColumn.ColumnType;
import org.apache.drill.exec.physical.impl.scan.project.ScanOutputColumn.ColumnsArrayColumn;
import org.apache.drill.exec.vector.ValueVector;

public class ColumnsArrayParser implements ScanProjectionParser {

  // Config

  private MajorType columnsArrayType;

  // Internals

  private ScanProjectionBuilder builder;

  // Output

  protected ColumnsArrayColumn columnsArrayCol;
  protected List<Integer> columnsIndexes;
  protected int maxIndex;

  private ColumnsArrayParser.ColumnsArrayProjection projection;

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
    if (! inCol.name().equalsIgnoreCase(ScanLevelProjection.COLUMNS_ARRAY_NAME)) {
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