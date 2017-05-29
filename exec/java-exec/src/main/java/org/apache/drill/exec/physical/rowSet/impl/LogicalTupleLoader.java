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
package org.apache.drill.exec.physical.rowSet.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.drill.exec.physical.rowSet.ColumnLoader;
import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.apache.drill.exec.physical.rowSet.TupleSchema;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;

/**
 * Shim inserted between an actual tuple loader and the client to remove columns
 * that are not projected from input to output. The underlying loader handles only
 * the projected columns in order to improve efficiency. This class presents the
 * full table schema, but returns null for the non-projected columns. This allows
 * the reader to work with the table schema as defined by the data source, but
 * skip those columns which are not projected. Skipping non-projected columns avoids
 * creating value vectors which are immediately discarded. It may also save the reader
 * from reading unwanted data.
 */
public class LogicalTupleLoader implements TupleLoader {

  /**
   * Implementation of the tuple schema that describes the full data source
   * schema. The underlying loader schema is a subset of these columns. Note
   * that the columns appear in the same order in both schemas, but the loader
   * schema is a subset of the table schema.
   */

  private class LogicalTupleSchema implements TupleSchema {

    private final Set<String> selection = new HashSet<>();
    private final TupleSchema physicalSchema;
    private final TupleNameSpace<MaterializedField> logicalSchema = new TupleNameSpace<>();

    private LogicalTupleSchema(TupleSchema physicalSchema, Collection<String> selection) {
      this.physicalSchema = physicalSchema;
      this.selection.addAll(selection);
    }

    @Override
    public int columnCount() { return logicalSchema.count(); }

    @Override
    public int columnIndex(String colName) {
      return logicalSchema.indexOf(rsLoader.toKey(colName));
    }

    @Override
    public MaterializedField column(int colIndex) { return logicalSchema.get(colIndex); }

    @Override
    public MaterializedField column(String colName) { return logicalSchema.get(colName); }

    @Override
    public int addColumn(MaterializedField columnSchema) {
      String key = rsLoader.toKey(columnSchema.getName());
      int lIndex = logicalSchema.add(columnSchema.getName(), columnSchema);
      if (selection.contains(key)) {
        addMapping(physicalSchema.addColumn(columnSchema));
      } else {
        addMapping(-1);
      }
      assert mapping.size() == lIndex + 1;
      return lIndex;
    }

    @Override
    public boolean selected(int colIndex) {
      return mapping.get(colIndex) != -1;
    }

    @Override
    public BatchSchema schema() {
      List<MaterializedField> fields = new ArrayList<>();
      for (MaterializedField col : logicalSchema) {
        fields.add(col);
      }
      return new BatchSchema(SelectionVectorMode.NONE, fields);
    }
  }

  private final ResultSetLoaderImpl rsLoader;
  private final LogicalTupleSchema schema;
  private final TupleLoader physicalLoader;
  private final List<Integer> mapping = new ArrayList<>();
  private ColumnLoader mappingCache[];

  public LogicalTupleLoader(ResultSetLoaderImpl rsLoader, TupleLoader physicalLoader, Collection<String> selection) {
    this.rsLoader = rsLoader;
    this.schema = new LogicalTupleSchema(physicalLoader.schema(), selection);
    this.physicalLoader = physicalLoader;
  }

  private void addMapping(int target) {
    mapping.add(target);
    mappingCache = null;
  }

  @Override
  public TupleSchema schema() { return schema; }

  @Override
  public ColumnLoader column(int colIndex) {
    if (mappingCache == null) {
      final int count = mapping.size();
      mappingCache = new ColumnLoader[count];
      for (int i = 0; i < count; i++) {
        int pIndex = mapping.get(i);
        if (pIndex != -1) {
          mappingCache[i] = physicalLoader.column(pIndex);
        }
      }
    }
    return mappingCache[colIndex];
  }

  @Override
  public ColumnLoader column(String colName) {
    int lIndex = schema.columnIndex(rsLoader.toKey(colName));
    return lIndex == -1 ? null : column(lIndex);
  }
}