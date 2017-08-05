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
package org.apache.drill.test.rowSet;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.spill.RecordBatchSizer;
import org.apache.drill.exec.physical.rowSet.model.TupleModel.RowSetModel;
import org.apache.drill.exec.physical.rowSet.model.simple.ReaderBuilderVisitor;
import org.apache.drill.exec.physical.rowSet.model.simple.RowSetModelImpl;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;

/**
 * Base class for row sets backed by a single record batch.
 */

public abstract class AbstractSingleRowSet extends AbstractRowSet implements SingleRowSet {

  public static class RowSetReaderBuilder extends ReaderBuilderVisitor {

    public RowSetReader buildReader(AbstractSingleRowSet rowSet, RowSetReaderIndex rowIndex) {
      RowSetModelImpl rowModel = rowSet.rowSetModelImpl();
      return new RowSetReaderImpl(rowModel.schema(), rowIndex, buildTuple(rowModel));
    }
  }

//  /**
//   * Wrapper around a primitive (non-map, non-list) column vector.
//   */
//
//  public static class PrimitiveColumnStorage extends ColumnStorage {
//    protected final ValueVector vector;
//
//    public PrimitiveColumnStorage(ColumnMetadata schema, ValueVector vector) {
//      super(schema);
//      this.vector = vector;
//    }
//
//    @Override
//    public AbstractObjectReader reader() {
//      return ColumnAccessorFactory.buildColumnReader(vector);
//    }
//
//    @Override
//    public void allocate(BufferAllocator allocator, int rowCount) {
//      // TODO: Use better estimates
//
//      AllocationHelper.allocate(vector, rowCount, 50, 10);
//    }
//
//    @Override
//    public AbstractObjectWriter writer() {
//      return ColumnAccessorFactory.buildColumnWriter(vector);
//    }
//  }
//
//  /**
//   * Wrapper around a map vector to provide both a column and tuple view of
//   * a single or repeated map.
//   */
//
//  public static class MapColumnStorage extends BaseMapColumnStorage {
//
//    private final AbstractMapVector vector;
//
//    public MapColumnStorage(ColumnMetadata schema, AbstractMapVector vector, ColumnStorage columns[]) {
//      super(schema, columns);
//      this.vector = vector;
//    }
//
//    public static MapColumnStorage fromMap(ColumnMetadata schema, AbstractMapVector vector) {
//      return new MapColumnStorage(schema, vector, buildColumns(schema, vector));
//    }
//
//    private static ColumnStorage[] buildColumns(ColumnMetadata schema, AbstractMapVector vector) {
//      TupleMetadata mapSchema = schema.mapSchema();
//      ColumnStorage columns[] = new ColumnStorage[mapSchema.size()];
//      for (int i = 0; i < mapSchema.size(); i++) {
//        ColumnMetadata colSchema = mapSchema.metadata(i);
//        @SuppressWarnings("resource")
//        ValueVector child = vector.getChildByOrdinal(i);
//        if (colSchema.structureType() == StructureType.TUPLE) {
//          columns[i] = MapColumnStorage.fromMap(colSchema, (AbstractMapVector) child);
//        } else {
//          columns[i] = new PrimitiveColumnStorage(colSchema, child);
//        }
//      }
//      return columns;
//    }
//
//    @Override
//    public AbstractObjectReader[] readers() {
//      return RowStorage.readers(this);
//    }
//
//    @Override
//    public List<AbstractObjectWriter> writers() {
//      return RowStorage.writers(this);
//    }
//
//    @Override
//    public void allocate(BufferAllocator allocator, int rowCount) {
//      RowStorage.allocate(this, allocator, rowCount);
//    }
//
//    @Override
//    public AbstractObjectWriter writer() {
//      if (schema.mode() == DataMode.REPEATED) {
//        RepeatedMapVector repeatedMapVector = (RepeatedMapVector) vector;
//        AbstractObjectWriter mapWriter = MapWriter.build(columnSchema(), repeatedMapVector, writers());
//        return ObjectArrayWriter.build(repeatedMapVector, mapWriter);
//      } else {
//        return MapWriter.build(columnSchema(), (MapVector) vector, writers());
//      }
//    }
//
//    @Override
//    public AbstractObjectReader reader() {
//      AbstractObjectReader mapReader = MapReader.build(columnSchema(), readers());
//      if (schema.mode() != DataMode.REPEATED) {
//        return mapReader;
//      }
//      return ObjectArrayReader.build((RepeatedMapVector) vector, mapReader);
//    }
//  }
//
//  /**
//   * Wrapper around a vector container to map the vector container into the common
//   * tuple format.
//   */
//
//  public static class RowStorage extends BaseRowStorage {
//
//    public RowStorage(TupleMetadata schema, VectorContainer container, ColumnStorage columns[]) {
//      super(schema, container, columns);
//    }
//
//    public static RowStorage fromSchema(BufferAllocator allocator, TupleMetadata schema) {
//      VectorContainer container = RowSetUtilities.buildVectors(allocator, schema);
//      return new RowStorage(schema, container, buildChildren(schema, container));
//    }
//
//    public static RowStorage fromContainer(TupleMetadata schema, VectorContainer container) {
//      return new RowStorage(schema, container, buildChildren(schema, container));
//    }
//
//    public static RowStorage fromContainer(VectorContainer container) {
//      return fromContainer(TupleSchema.fromFields(container.getSchema()), container);
//    }
//
//    private static ColumnStorage[] buildChildren(TupleMetadata schema, VectorContainer container) {
//      assert schema.size() == container.getNumberOfColumns();
//      ColumnStorage colStorage[] = new ColumnStorage[schema.size()];
//      for (int i = 0; i < schema.size(); i++) {
//        ColumnMetadata colSchema = schema.metadata(i);
//        @SuppressWarnings("resource")
//        ValueVector vector = container.getValueVector(i).getValueVector();
//        if (colSchema.structureType() == StructureType.TUPLE) {
//          colStorage[i] = MapColumnStorage.fromMap(colSchema, (AbstractMapVector) vector);
//        } else {
//          colStorage[i] = new PrimitiveColumnStorage(colSchema, vector);
//        }
//      }
//      return colStorage;
//    }
//
//    @Override
//    public AbstractObjectReader[] readers() {
//      return readers(this);
//    }
//
//    @Override
//    public List<AbstractObjectWriter> writers() {
//      return writers(this);
//    }
//
//    @Override
//    public void allocate(BufferAllocator allocator, int rowCount) {
//      allocate(this, allocator, rowCount);
//    }
//
//    protected static void allocate(TupleStorage storage, BufferAllocator allocator, int rowCount) {
//      for (int i = 0; i < storage.size(); i++) {
//        storage.storage(i).allocate(allocator, rowCount);
//      }
//    }
//
//    protected static List<AbstractObjectWriter> writers(AbstractRowSet.TupleStorage storage) {
//      List<AbstractObjectWriter> writers = new ArrayList<>();
//      for (int i = 0; i < storage.size();  i++) {
//        writers.add(storage.storage(i).writer());
//      }
//      return writers;
//    }
//  }

  private final RowSetModelImpl model;

  public AbstractSingleRowSet(AbstractSingleRowSet rowSet) {
    super(rowSet.allocator);
    model = rowSet.model;
  }

  public AbstractSingleRowSet(BufferAllocator allocator, RowSetModelImpl storage) {
    super(allocator);
    this.model = storage;
  }

  @Override
  public RowSetModel rowSetModel() { return model; }

  public RowSetModelImpl rowSetModelImpl() { return model; }

  @Override
  public int size() {
    RecordBatchSizer sizer = new RecordBatchSizer(container());
    return sizer.actualSize();
  }

  /**
   * Internal method to build the set of column readers needed for
   * this row set. Used when building a row set reader.
   * @param rowIndex object that points to the current row
   * @return an array of column readers: in the same order as the
   * (non-map) vectors.
   */

  protected RowSetReader buildReader(RowSetReaderIndex rowIndex) {
    return new RowSetReaderBuilder().buildReader(this, rowIndex);
  }
}
