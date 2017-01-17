package org.apache.drill.exec.store.revised;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.revised.Sketch.ColumnMaker;
import org.apache.drill.exec.store.revised.Sketch.ColumnSchema;
import org.apache.drill.exec.store.revised.Sketch.RowBatchMaker;
import org.apache.drill.exec.store.revised.Sketch.RowMaker;
import org.apache.drill.exec.store.revised.Sketch.RowSchema;
import org.apache.drill.exec.store.revised.Sketch.RowSetMaker;
import org.apache.drill.exec.store.revised.Sketch.ResultSetMaker;
import org.apache.drill.exec.store.revised.Sketch.RowBatch;
import org.apache.drill.exec.store.revised.Sketch.RowSchemaBuilder;
import org.apache.drill.exec.store.revised.VectorBuilder.VectorBuilderFactory;

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
      // TODO Auto-generated method stub
      return null;
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
