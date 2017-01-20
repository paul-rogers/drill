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
package org.apache.drill.exec.store.revised.exec;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.store.revised.Sketch;
import org.apache.drill.exec.store.revised.Sketch.ColumnMaker;
import org.apache.drill.exec.store.revised.Sketch.ColumnSchema;
import org.apache.drill.exec.store.revised.Sketch.RowBatchMaker;
import org.apache.drill.exec.store.revised.Sketch.RowMaker;
import org.apache.drill.exec.store.revised.Sketch.RowSchema;
import org.apache.drill.exec.store.revised.Sketch.RowSetMaker;
import org.apache.drill.exec.store.revised.retired.VectorBuilder;
import org.apache.drill.exec.store.revised.retired.VectorBuilder.VectorBuilderFactory;
import org.apache.drill.exec.store.revised.Sketch.ResultSetMaker;
import org.apache.drill.exec.store.revised.Sketch.RowBatch;
import org.apache.drill.exec.store.revised.Sketch.RowSchemaBuilder;
import org.apache.drill.exec.store.revised.schema.RowSchemaBuilderImpl;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.exec.vector.ValueVector;

public class ResultSetMakerImpl implements ResultSetMaker {

  protected static class ScanState {
    private ResultSetMakerImpl receiver;
    protected int rowIndex;

    protected ScanState(ResultSetMakerImpl receiver) {
      this.receiver = receiver;
    }
    protected ResultSetMakerImpl receiver() { return receiver; }
    protected int rowIndex( ) { return rowIndex; }
  }

//  public static class RowSetBuilderImpl implements RowSetBuilder {
//
//    private RowSchema schema;
//    private final ResultSetMakerImpl scanReceiver;
//
//    public RowSetBuilderImpl(ResultSetMakerImpl scanReceiver) {
//      this.scanReceiver = scanReceiver;
//    }
//
//    public RowSetBuilderImpl(ResultSetMakerImpl scanReceiver,
//        RowSchema schema) {
//      this.scanReceiver = scanReceiver;
//      this.schema = schema;
//    }
//
//    @Override
//    public SchemaBuilder schema() {
//      schema = null;
//      SchemaBuilderImpl builder;
//      if (schema == null) {
//        builder = new SchemaBuilderImpl( );
//      } else {
//        builder = new SchemaBuilderImpl( schema );
//      }
//      builder.listener( new SchemaBuilderImpl.Listener() {
//
//        @Override
//        public void onBuild(RowSchema schema) {
//          RowSetBuilderImpl.this.schema = schema;
//        }
//      } );
//      return builder;
//    }
//
//    @Override
//    public RowSetMaker build() {
//      if (schema == null) {
//        throw new IllegalStateException( "No schema provided" );
//      }
//      RowSetReceiverImpl rowSetReceiver = new RowSetReceiverImpl( scanReceiver, schema );
//      scanReceiver.setActiveRowSet(rowSetReceiver);
//      return rowSetReceiver;
//    }
//
//  }

  /**
   * Buffers a row of data prior to inserting it in a vector.
   */

//  public static class BufferedRowMakerImpl implements RowMaker {
//
//    private final Object rowBuffer[];
//
//    private BufferedRowMakerImpl(BufferedBatchMakerImpl batchMaker) {
//      int rowWidth = batchMaker.rowSchema().size();
//      rowBuffer = new Object[rowWidth];
//    }
//
//    @Override
//    public ColumnMaker column(int index) {
//      // TODO Auto-generated method stub
//      return null;
//    }
//
//    @Override
//    public ColumnMaker column(String path) {
//      // TODO Auto-generated method stub
//      return null;
//    }
//
//    @Override
//    public void accept() {
//      batchMaker.addRow( rowBuffer );
//    }
//
//    @Override
//    public void reject() {
//      // Nothing to do, just let Java garbage collect the buffer
//    }
//  }

  public static class DirectRowMakerImpl implements RowMaker {

    private ColumnMaker columnMakers[];

    public DirectRowMakerImpl(DirectRowBatchMakerImpl batchMaker) {
      BatchMutator mutator = batchMaker.mutator();
      int colCount = mutator.vectorCount();
      columnMakers = new ColumnMaker[colCount];
      for (int i = 0; i < colCount; i++) {
        columnMakers[i] = mutator.columnMaker(i);
      }
    }

    @Override
    public ColumnMaker column(int index) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public ColumnMaker column(String path) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void accept() {
      // TODO Auto-generated method stub

    }

    @Override
    public void reject() {
      // TODO Auto-generated method stub

    }

  }
//public static class DirectRowBuilderImpl implements RowBuilder {
//
//  @Override
//  public ColumnWriter column(int index) {
//    // TODO Auto-generated method stub
//    return null;
//  }
//
//  @Override
//  public ColumnWriter column(String path) {
//    // TODO Auto-generated method stub
//    return null;
//  }
//
//  @Override
//  public void accept() {
//    // TODO Auto-generated method stub
//
//  }
//
//  @Override
//  public void reject() {
//    // TODO Auto-generated method stub
//
//  }
//
//}
//
//public static class RowSetBuilderImpl implements RowSetBuilder {
//
//  @Override
//  public RowBuilder row() {
//    // TODO Auto-generated method stub
//    return null;
//  }
//
//  @Override
//  public boolean full() {
//    // TODO Auto-generated method stub
//    return false;
//  }
//
//  @Override
//  public void build() {
//    // TODO Auto-generated method stub
//
//  }
//
//}
//
//  public static class BufferedBatchMakerImpl implements RowBatchMaker {
//
//    private List<Object[]> rows = new ArrayList<>();
//    private RowSetMakerImpl rowSet;
//
//    public BufferedBatchMakerImpl(RowSetMakerImpl rowSet) {
//      this.rowSet = rowSet;
//    }
//
//    @Override
//    public RowMaker row() {
//      return new BufferedRowMakerImpl(this);
//    }
//
//    @Override
//    public int rowCount() {
//      return rows.size();
//    }
//
//    @Override
//    public boolean full() {
//      // TODO Auto-generated method stub
//      return false;
//    }
//
//    @Override
//    public void abandon() {
//      // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public RowBatch build() {
//      // TODO Auto-generated method stub
//      return null;
//    }
//
//    @Override
//    public RowSchema rowSchema() {
//      return rowSet.rowSchema();
//    }
//
//  }

  public static class IntColumnMaker extends AbstractColumnMaker {

    IntVector.Mutator mutator;

    @Override
    protected void bind(BatchMutator batchMutator, int index) {
      super.bind(batchMutator, index);
      @SuppressWarnings("resource")
      IntVector vector = batchMutator.vector(index);
      this.mutator = vector.getMutator();
    }

    @Override
    public void setInt(int value) {
      mutator.set(rowIndex(), value);
    }
  }

  public static class NullableIntColumnMaker extends AbstractColumnMaker {

    NullableIntVector.Mutator mutator;

    @Override
    protected void bind(BatchMutator batchMutator, int index) {
      super.bind(batchMutator, index);
      @SuppressWarnings("resource")
      NullableIntVector vector = batchMutator.vector(index);
      this.mutator = vector.getMutator();
    }

    @Override
    public void setInt(int value) {
      mutator.set(rowIndex(), value);
    }

    @Override
    public void setNull() {
      mutator.setNull(rowIndex());
    }
  }

  public static class ColumnMakerFactory {

    private static final ColumnMakerFactory instance = new ColumnMakerFactory();

    private final Class<? extends AbstractColumnMaker> columnMakers[][];

    @SuppressWarnings("unchecked")
    private ColumnMakerFactory() {
      columnMakers = new Class[MinorType.values().length][];
      int modeCount = DataMode.values().length;
      for (int i=0; i < columnMakers.length; i++) {
        columnMakers[i] = new Class[modeCount];
      }

      registerSpecialMakers();
    }

    private void registerSpecialMakers() {
      register(MinorType.INT, DataMode.REQUIRED, IntColumnMaker.class);
      register(MinorType.INT, DataMode.OPTIONAL, NullableIntColumnMaker.class);
    }

    public void register(MinorType type, DataMode mode, Class<? extends AbstractColumnMaker> columnMaker) {
      assert columnMakers[type.ordinal()][mode.ordinal()] == null;
      columnMakers[type.ordinal()][mode.ordinal()] = columnMaker;
    }

    public static AbstractColumnMaker newColumnMaker(ColumnSchema colSchema) {
      return newColumnMaker(colSchema.type(), colSchema.cardinality());
    }

    public static AbstractColumnMaker newColumnMaker(MinorType type, DataMode mode) {
      Class<? extends AbstractColumnMaker> columnMakerClass = instance.columnMakers[type.ordinal()][mode.ordinal()];
      if (columnMakerClass == null) {
        throw new IllegalStateException(
            String.format("Unsupported ColumnMaker: type = %s, mode = %s", type, mode));
      }
      try {
        return columnMakerClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new IllegalStateException(
            String.format("Unable to create ColumnMaker: type = %s, mode = %s", type, mode));
      }
    }
  }

  public static class BatchMutator {
    private RowSchema schema;
    BufferAllocator allocator;
    int maxRows;
    MaterializedField fields[];
    ValueVector vectors[];
    int fieldWidths[];
    private SchemaChangeCallBack callBack = new SchemaChangeCallBack();
    int rowCount;

    public BatchMutator(RowSchema schema, BufferAllocator allocator, int maxRows) {
      this.schema = schema;
      this.allocator = allocator;
      fields = new MaterializedField[schema.size()];
      vectors = new ValueVector[schema.size()];
      fieldWidths = new int[schema.size()];
      for (int i = 0; i < schema.size(); i++ ) {
        fields[i] = schema.column(i).materializedField();
        vectors[i] = TypeHelper.getNewVector(fields[i], allocator, callBack);
        fieldWidths[i] = schema.colWidth();
      }
    }

    public int vectorCount() {
      return vectors.length;
    }

    @SuppressWarnings("unchecked")
    public <T extends ValueVector> T vector(int i) {
      return (T) vectors[i];
    }

    public void allocate() {
      for (int i = 0; i < vectors.length; i++) {
        AllocationHelper.allocate(vectors[i], maxRows, fieldWidths[i], 10);
      }
    }

    public ColumnMaker columnMaker(int colIndex) {
      AbstractColumnMaker colMaker = ColumnMakerFactory.newColumnMaker(schema.column(colIndex));
      colMaker.bind(this, colIndex);
      return colMaker;
    }

    public boolean isFull() {
      return rowCount >= maxRows;
    }

    public void bindToContainer(VectorContainer container) {
      container.clear();
      for (int i = 0; i < vectors.length; i++) {
        container.add(vectors[i]);
      }
      container.buildSchema(SelectionVectorMode.NONE);
    }

    public int rowIndex() {
      return rowCount;
    }
  }

  public static class DirectRowBatchMakerImpl implements RowBatchMaker {

    private static final int FIX_ME = 1000;
    private RowSetMakerImpl rowSet;
    private BatchMutator mutator;
    private DirectRowMakerImpl rowMaker;
    private int rowCount;
    private int targetCount;

    public DirectRowBatchMakerImpl(RowSetMakerImpl rowSet) {
      this.rowSet = rowSet;
      mutator = new BatchMutator(rowSchema(), rowSet.allocator(), FIX_ME);
      rowMaker = new DirectRowMakerImpl(this);
    }

    protected BatchMutator mutator() {
      return mutator;
    }

    @Override
    public RowSchema rowSchema() {
      return rowSet.rowSchema();
    }

    @Override
    public RowMaker row() {
      return rowMaker;
    }

    @Override
    public int rowCount() {
      return rowCount;
    }

    @Override
    public boolean full() {
      return rowCount >= targetCount;
    }

    @Override
    public void abandon() {
      // TODO Auto-generated method stub

    }

    @Override
    public RowBatch build() {
      // TODO Auto-generated method stub
      return null;
    }

  }
  public static class RowSetMakerImpl implements RowSetMaker {

    private final ResultSetMakerImpl scanReceiver;
    private final RowSchema schema;
    private final RowSchema flatSchema;
    private RowBatchMakerImpl activeBatch;
    protected VectorBuilder columns[];
    MaterializedField fields[];
    private long rowCount;
    private final ScanState scanState;

    public RowSetMakerImpl(ResultSetMakerImpl scanReceiver, RowSchema schema) {
      this.scanReceiver = scanReceiver;
      this.schema = schema;
      flatSchema = schema.flatten();
      scanState = scanReceiver.scanState();
      columns = new VectorBuilder[flatSchema.size()];
      fields = new MaterializedField[flatSchema.size()];
      for ( int i = 0; i < flatSchema.size(); i++ ) {
        ColumnSchema colSchema = flatSchema.column(i);
        fields[i] = colSchema.materializedField();
        columns[i] = VectorBuilderFactory.newBuilder(colSchema.majorType());
        columns[i].bind(scanState);
      }
    }
//
//    // TODO: Should be done internally, then handed to scan batch
//
//    public void createVectors(OutputMutator batchMutator) throws SchemaChangeException {
//      for ( int i = 0; i < columns.length;  i++ ) {
//        columns[i].build(batchMutator, fields[i]);
//      }
//    }

    @Override
    public RowSchema rootSchema() {
      return schema;
    }

    @Override
    public RowSchema rowSchema() {
      return flatSchema;
    }

//    @Override
//    public RowBatchMaker batch() {
//      if ( ! scanReceiver.isActiveRowSet(this) ) {
//        throw new IllegalStateException( "Not the active row set" );
//      }
//      if (activeBatch != null) {
//        activeBatch.close( );
//      }
//      activeBatch = new RowBatchReceiverImpl( this );
//      return activeBatch;
//    }
//
//    protected void clearActiveBatch() {
//      activeBatch = null;
//    }
//
////    @Override
////    public RowSetBuilder reviseSchema() {
////      scanReceiver.assertOpen();
////      close();
////      return new RowSetBuilderImpl( scanReceiver, schema );
////    }
//
//    @Override
//    public void close() {
//      if ( ! scanReceiver.isActiveRowSet(this) ) {
//        return; }
//      if (activeBatch != null) {
//        activeBatch.close();
//      }
//      scanReceiver.setActiveRowSet(null);
//    }

    protected boolean isActiveBatch(RowBatchMakerImpl batch) {
      return activeBatch == batch;
    }

//    public int rowIndex() {
//      return rowCount;
//    }

    @Override
    public long rowCount() {
      long total = rowCount;
      if (activeBatch != null)
        total += activeBatch.rowCount();
      return total;
    }

    @Override
    public RowBatchMaker batch() {
      closeActiveBatch();
      activeBatch = new RowBatchMakerImpl(this);
      return activeBatch;
    }

    public void close() {
      closeActiveBatch();
    }

    private void closeActiveBatch() {
      if (activeBatch != null) {
        activeBatch.close();
      }
      activeBatch = null;
    }

  }

  public static class RowBatchMakerImpl implements RowBatchMaker
  {

    private RowSetMakerImpl rowSet;

    public RowBatchMakerImpl(RowSetMakerImpl rowSet) {
      this.rowSet = rowSet;
    }

    @Override
    public RowMaker row() {
      if ( full( ) ) {
        return null; }
      if ( ! rowSet.isActiveBatch(this)) {
        throw new IllegalStateException( "Not the active batch" );
      }
      return new RowMakerImpl(this);
    }

    @Override
    public boolean full() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public int rowCount() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public RowBatch build() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void abandon() {
      // TODO Auto-generated method stub

    }

    public void close() {
      abandon();
    }

  }

  private enum State { START, ACTIVE, CLOSED; }
  private State state = State.START;
  private final ScanState scanState;
  private long batchCount;
  private long totalRowCount;
  private RowSetMaker currentRowSet;

  public ResultSetMakerImpl() {
    scanState = new ScanState(this);
  }

  protected ScanState scanState() {
    return scanState;
  }

  public boolean isActiveRowSet(RowSetMaker rowSet) {
    return currentRowSet == rowSet;
  }

  protected void assertOpen() {
    if (state == State.CLOSED) {
      throw new IllegalStateException( "The scan is already closed" );
    }
  }

  @Override
  public void close() {
    if (state != State.ACTIVE) {
      return; }
    closeRowSet();
    state = State.CLOSED;
  }

  @Override
  public RowSetMaker rowSet(RowSchema schema) {
    closeRowSet();
    currentRowSet = new RowSetMakerImpl(this, schema);
    return currentRowSet;
  }

  private void closeRowSet() {
    if (currentRowSet != null) {
      currentRowSet = null;
    }
    currentRowSet = null;
  }

  @Override
  public long rowCount() {
    return totalRowCount;
  }

  @Override
  public RowSchemaBuilder newSchema() {
    return new RowSchemaBuilderImpl();
  }

  protected void batchAccepted(RowBatch batch) {
    batchCount++;
    totalRowCount += batch.rowCount();
  }

  @Override
  public long batchCount() {
    return batchCount;
  }

  @Override
  public void abandon() {
    // TODO Auto-generated method stub

  }

}
