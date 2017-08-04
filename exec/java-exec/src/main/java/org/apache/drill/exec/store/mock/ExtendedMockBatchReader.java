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
package org.apache.drill.exec.store.mock;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.physical.impl.scan.RowBatchReader;
import org.apache.drill.exec.physical.impl.scan.SchemaNegotiator;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.store.mock.MockTableDef.MockColumn;
import org.apache.drill.exec.store.mock.MockTableDef.MockScanEntry;
import org.apache.drill.exec.vector.ValueVector;

/**
 * Extended form of the mock record reader that uses generator class
 * instances to create the mock values. This is a work in progress.
 * Generators exist for a few simple required types. One also exists
 * to generate strings that contain dates.
 * <p>
 * The definition is provided inside the sub scan used to create the
 * {@link ScanBatch} used to create this record reader.
 */

public class ExtendedMockBatchReader implements RowBatchReader {

  private final MockScanEntry config;
  private final ColumnDef fields[];
  private ResultSetLoader mutator;
  private TupleLoader writer;

  public ExtendedMockBatchReader(MockScanEntry config) {
    this.config = config;
    fields = buildColumnDefs();
  }

  private ColumnDef[] buildColumnDefs() {
    List<ColumnDef> defs = new ArrayList<>();

    // Look for duplicate names. Bad things happen when the same name
    // appears twice. We must do this here because some tests create
    // a physical plan directly, meaning that this is the first
    // opportunity to review the column definitions.

    Set<String> names = new HashSet<>();
    MockColumn cols[] = config.getTypes();
    for (int i = 0; i < cols.length; i++) {
      MockTableDef.MockColumn col = cols[i];
      if (names.contains(col.name)) {
        throw new IllegalArgumentException("Duplicate column name: " + col.name);
      }
      names.add(col.name);
      int repeat = Math.max(1, col.getRepeatCount());
      if (repeat == 1) {
        defs.add(new ColumnDef(col));
      } else {
        for (int j = 0; j < repeat; j++) {
          defs.add(new ColumnDef(col, j+1));
        }
      }
    }
    ColumnDef[] defArray = new ColumnDef[defs.size()];
    defs.toArray(defArray);
    return defArray;
  }


  @Override
  public boolean open(SchemaNegotiator schemaNegotiator) {
    TupleMetadata schema = new TupleMetadata();
    for (int i = 0; i < fields.length; i++) {
      final ColumnDef col = fields[i];
      final MaterializedField field = MaterializedField.create(col.getName(),
                                          col.getConfig().getMajorType());
      schema.add(field);
    }
    schemaNegotiator.setTableSchema(schema);
    mutator = schemaNegotiator.build();
    writer = mutator.root();
    for (int i = 0; i < fields.length; i++) {
      fields[i].generator.setup(fields[i], writer.column(i));
    }
    return true;
  }

  @Override
  public boolean next() {
    int rowCount = config.getRecords() - mutator.totalRowCount();
    if (rowCount <= 0) {
      return false;
    }

    rowCount = Math.min(rowCount, ValueVector.MAX_ROW_COUNT);
    if (config.getBatchSize() > 0) {
      rowCount = Math.min(rowCount, config.getBatchSize());
    }
    Random rand = new Random();
    for (int i = 0; i < rowCount; i++) {
      if (mutator.isFull()) {
        break;
      }
      mutator.startRow();
      for (int j = 0; j < fields.length; j++) {
        if (fields[j].nullable && rand.nextInt(100) < fields[j].nullablePercent) {
          writer.column(j).setNull();
        } else {
          fields[j].generator.setValue();
        }
      }
      mutator.saveRow();
    }

    return true;
  }

//  @Override
//  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
//    try {
//      for (final ValueVector v : vectorMap.values()) {
//        AllocationHelper.allocate(v, Character.MAX_VALUE, 50, 10);
//      }
//    } catch (NullPointerException e) {
//      throw new OutOfMemoryException();
//    }
//  }

  @Override
  public void close() { }
}