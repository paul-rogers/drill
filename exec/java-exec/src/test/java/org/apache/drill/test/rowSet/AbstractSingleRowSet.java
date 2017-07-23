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

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.spill.RecordBatchSizer;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.record.TupleMetadata.StructureType;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.impl.AbstractColumnReader;
import org.apache.drill.exec.vector.accessor.impl.ColumnAccessorFactory;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractTupleWriter;
import org.apache.drill.exec.vector.accessor.writer.MapWriter;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;

/**
 * Base class for row sets backed by a single record batch.
 */

public abstract class AbstractSingleRowSet extends AbstractRowSet implements SingleRowSet {

  /**
   * Common interface to access a tuple backed by a vector container or a
   * map vector.
   */

  public interface TupleStorage {
    TupleMetadata tupleSchema();
    int size();
    ValueVector vector(int index);
    ColumnStorage storage(int index);
    AbstractColumnReader[] readers(RowSetReaderIndex rowIndex);
    AbstractObjectWriter[] writers(ColumnWriterIndex rowIndex);
    void allocate(BufferAllocator allocator, int rowCount);
  }

  /**
   * Represents a column within a tuple, including the tuple metadata
   * and column storage. A wrapper around a vector to include metadata
   * and handle nested tuples.
   */

  public static abstract class ColumnStorage {
    protected final ColumnMetadata schema;

    public ColumnStorage(ColumnMetadata schema) {
      this.schema = schema;
    }

    public ColumnMetadata columnSchema() { return schema; }
    public abstract ValueVector vector();
    public abstract AbstractColumnReader reader(RowSetReaderIndex index);
    public abstract AbstractObjectWriter writer(ColumnWriterIndex rowIndex);
    public abstract void allocate(BufferAllocator allocator, int rowCount);
  }

  /**
   * Wrapper around a primitive (non-map, non-list) column vector.
   */

  public static class PrimitiveColumnStorage extends ColumnStorage {
    protected final ValueVector vector;

    public PrimitiveColumnStorage(ColumnMetadata schema, ValueVector vector) {
      super(schema);
      this.vector = vector;
    }

    @Override
    public ValueVector vector() { return vector; }

    @Override
    public AbstractColumnReader reader(RowSetReaderIndex index) {
      AbstractColumnReader reader = ColumnAccessorFactory.newReader(vector.getField().getType());
      reader.bind(index, vector);
      return reader;
    }

    @Override
    public void allocate(BufferAllocator allocator, int rowCount) {
      // TODO: Use better estimates

      AllocationHelper.allocate(vector, rowCount, 50, 10);
    }

    @Override
    public AbstractObjectWriter writer(ColumnWriterIndex rowIndex) {
      return ColumnAccessorFactory.buildColumnWriter(rowIndex, vector);
    }
  }

  /**
   * Wrapper around a map vector to provide both a column and tuple view of
   * the map.
   */

  public static class MapColumnStorage extends ColumnStorage implements TupleStorage {

    private final AbstractMapVector vector;
    private final ColumnStorage columns[];

    public MapColumnStorage(ColumnMetadata schema, AbstractMapVector vector, ColumnStorage columns[]) {
      super(schema);
      this.vector = vector;
      this.columns = columns;
    }

    public static MapColumnStorage fromMap(ColumnMetadata schema, AbstractMapVector vector) {
      return new MapColumnStorage(schema, vector, buildColumns(schema, vector));
    }

    private static ColumnStorage[] buildColumns(ColumnMetadata schema, AbstractMapVector vector) {
      TupleMetadata mapSchema = schema.mapSchema();
      ColumnStorage columns[] = new ColumnStorage[mapSchema.size()];
      for (int i = 0; i < mapSchema.size(); i++) {
        ColumnMetadata colSchema = mapSchema.metadata(i);
        @SuppressWarnings("resource")
        ValueVector child = vector.getChildByOrdinal(i);
        if (colSchema.structureType() == StructureType.TUPLE) {
          columns[i] = MapColumnStorage.fromMap(colSchema, (AbstractMapVector) child);
        } else {
          columns[i] = new PrimitiveColumnStorage(colSchema, child);
        }
      }
      return columns;
    }

    private static MapColumnStorage flatten(ColumnMetadata destMd, MapColumnStorage sourceMapStorage) {
      assert destMd.name().equals(sourceMapStorage.columnSchema().name());
      List<ColumnStorage> destCols = new ArrayList<>();
      flattenTuple(sourceMapStorage, destMd.mapSchema(), destCols);
      return new MapColumnStorage(destMd, (AbstractMapVector) sourceMapStorage.vector(),
                                  destCols.toArray(new ColumnStorage[destCols.size()]));
    }

    @Override
    public int size() { return columns.length; }

    @Override
    public TupleMetadata tupleSchema() { return schema.mapSchema(); }

    @Override
    public ValueVector vector(int index) {
      return columns[index].vector();
    }

    @Override
    public ValueVector vector() { return vector; }

    @Override
    public ColumnStorage storage(int index) { return columns[index]; }

    @Override
    public AbstractColumnReader reader(RowSetReaderIndex index) {
      throw new UnsupportedOperationException();
    }

    @Override
    public AbstractColumnReader[] readers(RowSetReaderIndex rowIndex) {
      return RowStorage.readers(this, rowIndex);
    }

    @Override
    public AbstractObjectWriter[] writers(ColumnWriterIndex rowIndex) {
      return RowStorage.writers(this, rowIndex);
    }

    @Override
    public void allocate(BufferAllocator allocator, int rowCount) {
      RowStorage.allocate(this, allocator, rowCount);
    }

    @Override
    public AbstractObjectWriter writer(ColumnWriterIndex rowIndex) {
      AbstractObjectWriter[] writers = writers(rowIndex);
      MapWriter mapWriter = new MapWriter(tupleSchema(), writers);
      return new AbstractTupleWriter.TupleObjectWriter(mapWriter);
    }
  }

  /**
   * Wrapper around a vector container to map the vector container into the common
   * tuple format.
   */

  public static class RowStorage implements TupleStorage {
    private final TupleMetadata schema;
    private final ColumnStorage columns[];
    private final VectorContainer container;

    public RowStorage(TupleMetadata schema, VectorContainer container, ColumnStorage columns[]) {
      this.schema = schema;
      this.columns = columns;
      this.container = container;
    }

    public static RowStorage fromSchema(BufferAllocator allocator, TupleMetadata schema) {
      VectorContainer container = RowSetUtilities.buildVectors(allocator, schema);
      return new RowStorage(schema, container, buildChildren(schema, container));
    }

    public static RowStorage fromContainer(TupleMetadata schema, VectorContainer container) {
      return new RowStorage(schema, container, buildChildren(schema, container));
    }

    private static ColumnStorage[] buildChildren(TupleMetadata schema, VectorContainer container) {
      assert schema.size() == container.getNumberOfColumns();
      ColumnStorage colStorage[] = new ColumnStorage[schema.size()];
      for (int i = 0; i < schema.size(); i++) {
        ColumnMetadata colSchema = schema.metadata(i);
        @SuppressWarnings("resource")
        ValueVector vector = container.getValueVector(i).getValueVector();
        if (colSchema.structureType() == StructureType.TUPLE) {
          colStorage[i] = MapColumnStorage.fromMap(colSchema, (AbstractMapVector) vector);
        } else {
          colStorage[i] = new PrimitiveColumnStorage(colSchema, vector);
        }
      }
      return colStorage;
    }

    public static RowStorage flattened(RowStorage sourceStorage) {
      TupleMetadata destSchema = sourceStorage.tupleSchema().flatten();
      List<ColumnStorage> destCols = new ArrayList<>();
      flattenTuple(sourceStorage, destSchema, destCols);
      return new RowStorage(destSchema, null, destCols.toArray(new ColumnStorage[destCols.size()]));
    }

    @Override
    public int size() { return columns.length; }

    @Override
    public TupleMetadata tupleSchema() { return schema; }

    @Override
    public ValueVector vector(int index) {
      return columns[index].vector();
    }

    @Override
    public ColumnStorage storage(int index) { return columns[index]; }

    public VectorContainer container() { return container; }

    public RowStorage flatten() {
      return RowStorage.flattened(this);
    }

    @Override
    public AbstractColumnReader[] readers(RowSetReaderIndex rowIndex) {
      return readers(this, rowIndex);
    }

    @Override
    public AbstractObjectWriter[] writers(ColumnWriterIndex rowIndex) {
      return writers(this, rowIndex);
    }

    @Override
    public void allocate(BufferAllocator allocator, int rowCount) {
      allocate(this, allocator, rowCount);
    }

    protected static AbstractColumnReader[] readers(TupleStorage storage, RowSetReaderIndex rowIndex) {
      AbstractColumnReader[] readers = new AbstractColumnReader[storage.tupleSchema().size()];
      for (int i = 0; i < readers.length; i++) {
        readers[i] = storage.storage(i).reader(rowIndex);
      }
      return readers;
    }

    protected static AbstractObjectWriter[] writers(TupleStorage storage, ColumnWriterIndex rowIndex) {
      AbstractObjectWriter[] writers = new AbstractObjectWriter[storage.size()];
      for (int i = 0; i < writers.length;  i++) {
        writers[i] = storage.storage(i).writer(rowIndex);
      }
      return writers;
    }

    protected static void allocate(TupleStorage storage, BufferAllocator allocator, int rowCount) {
      for (int i = 0; i < storage.size(); i++) {
        storage.storage(i).allocate(allocator, rowCount);
      }
    }
  }

  protected final RowStorage rowStorage;

  public AbstractSingleRowSet(BufferAllocator allocator, TupleMetadata schema) {
    super(allocator, schema, new VectorContainer());
    rowStorage = RowStorage.fromSchema(allocator, schema);
  }

  public AbstractSingleRowSet(BufferAllocator allocator, VectorContainer container) {
    super(allocator, TupleMetadata.fromFields(container.getSchema()), container);
    rowStorage = RowStorage.fromContainer(schema, container);
  }

  public AbstractSingleRowSet(AbstractSingleRowSet rowSet) {
    super(rowSet.allocator, rowSet.schema, rowSet.container);
    rowStorage = rowSet.rowStorage;
  }

  @Override
  public int size() {
    RecordBatchSizer sizer = new RecordBatchSizer(container);
    return sizer.actualSize();
  }

  /**
   * Given a tuple storage representing a non-flattened row or map, return
   * a flattened version of the tuple in which all columns except repeated
   * maps are pushed up to the root level. Recursively apply these rule for
   * nested maps.
   * @param sourceStorage the original, non-flattened row or map
   * @param destSchema the schema for the destination, flattened tuple
   * @param destCols the columns for the flattened tuple
   */
  private static void flattenTuple(TupleStorage sourceStorage, TupleMetadata destSchema, List<ColumnStorage> destCols) {
    TupleMetadata sourceSchema = sourceStorage.tupleSchema();
    for (int i = 0; i < sourceSchema.size(); i++) {
      ColumnMetadata sourceMd = sourceSchema.metadata(i);
      ColumnStorage sourceColStorage = sourceStorage.storage(sourceMd.base().index());
      int destIndex = destCols.size();
      if (sourceMd.type() != MinorType.MAP) {
        ColumnMetadata destMd = destSchema.metadata(destIndex);
        assert destMd.name().equals(sourceMd.name());
        destCols.add(new PrimitiveColumnStorage(destMd, sourceColStorage.vector()));
      } else if (sourceMd.mode() == DataMode.REPEATED) {
        ColumnMetadata destMd = destSchema.metadata(destIndex);
        destCols.add(MapColumnStorage.flatten(destMd, (MapColumnStorage) sourceColStorage));
      } else {
        flattenTuple((MapColumnStorage) sourceColStorage, destSchema, destCols);
      }
    }
  }

  /**
   * Internal method to build the set of column readers needed for
   * this row set. Used when building a row set reader.
   * @param rowIndex object that points to the current row
   * @return an array of column readers: in the same order as the
   * (non-map) vectors.
   */

  protected RowSetReader buildReader(RowSetReaderIndex rowIndex) {
    RowStorage flattened = rowStorage.flatten();
    return new RowSetReaderImpl(flattened.tupleSchema(), rowIndex, flattened.readers(rowIndex));
  }
}
