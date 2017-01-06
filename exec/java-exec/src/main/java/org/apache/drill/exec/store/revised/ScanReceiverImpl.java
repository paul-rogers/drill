package org.apache.drill.exec.store.revised;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.revised.Sketch.ColumnBuilder;
import org.apache.drill.exec.store.revised.Sketch.ColumnSchema;
import org.apache.drill.exec.store.revised.Sketch.RowBatchReceiver;
import org.apache.drill.exec.store.revised.Sketch.RowBuilder;
import org.apache.drill.exec.store.revised.Sketch.RowSchema;
import org.apache.drill.exec.store.revised.Sketch.RowSetBuilder;
import org.apache.drill.exec.store.revised.Sketch.RowSetReceiver;
import org.apache.drill.exec.store.revised.Sketch.ScanReceiver;
import org.apache.drill.exec.store.revised.Sketch.SchemaBuilder;
import org.apache.drill.exec.store.revised.VectorBuilder.VectorBuilderFactory;

public class ScanReceiverImpl implements ScanReceiver {

  protected static class ScanState {
    private ScanReceiverImpl receiver;
    protected int rowIndex;

    protected ScanState(ScanReceiverImpl receiver) {
      this.receiver = receiver;
    }
    protected ScanReceiverImpl receiver() { return receiver; }
    protected int rowIndex( ) { return rowIndex; }
  }

  public static class RowSetBuilderImpl implements RowSetBuilder {

    private RowSchema schema;
    private final ScanReceiverImpl scanReceiver;

    public RowSetBuilderImpl(ScanReceiverImpl scanReceiver) {
      this.scanReceiver = scanReceiver;
    }

    public RowSetBuilderImpl(ScanReceiverImpl scanReceiver,
        RowSchema schema) {
      this.scanReceiver = scanReceiver;
      this.schema = schema;
    }

    @Override
    public SchemaBuilder schema() {
      schema = null;
      SchemaBuilderImpl builder;
      if (schema == null) {
        builder = new SchemaBuilderImpl( );
      } else {
        builder = new SchemaBuilderImpl( schema );
      }
      builder.listener( new SchemaBuilderImpl.Listener() {

        @Override
        public void onBuild(RowSchema schema) {
          RowSetBuilderImpl.this.schema = schema;
        }
      } );
      return builder;
    }

    @Override
    public RowSetReceiver build() {
      if (schema == null) {
        throw new IllegalStateException( "No schema provided" );
      }
      RowSetReceiverImpl rowSetReceiver = new RowSetReceiverImpl( scanReceiver, schema );
      scanReceiver.setActiveRowSet(rowSetReceiver);
      return rowSetReceiver;
    }

  }

  public static class RowSetReceiverImpl implements RowSetReceiver {

    private final ScanReceiverImpl scanReceiver;
    private final RowSchema schema;
    private final RowSchema flatSchema;
    private RowBatchReceiverImpl activeBatch;
    protected VectorBuilder columns[];
    MaterializedField fields[];
    private int rowCount;
    private final ScanState scanState;

    public RowSetReceiverImpl(ScanReceiverImpl scanReceiver, RowSchema schema) {
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

    // TODO: Should be done internally, then handed to scan batch

    public void createVectors(OutputMutator batchMutator) throws SchemaChangeException {
      for ( int i = 0; i < columns.length;  i++ ) {
        columns[i].build(batchMutator, fields[i]);
      }
    }

    @Override
    public RowSchema rootSchema() {
      return schema;
    }

    @Override
    public RowSchema rowSchema() {
      return flatSchema;
    }

    @Override
    public RowBatchReceiver batch() {
      if ( ! scanReceiver.isActiveRowSet(this) ) {
        throw new IllegalStateException( "Not the active row set" );
      }
      if (activeBatch != null) {
        activeBatch.close( );
      }
      activeBatch = new RowBatchReceiverImpl( this );
      return activeBatch;
    }

    protected void clearActiveBatch() {
      activeBatch = null;
    }

//    @Override
//    public RowSetBuilder reviseSchema() {
//      scanReceiver.assertOpen();
//      close();
//      return new RowSetBuilderImpl( scanReceiver, schema );
//    }

    @Override
    public void close() {
      if ( ! scanReceiver.isActiveRowSet(this) ) {
        return; }
      if (activeBatch != null) {
        activeBatch.close();
      }
      scanReceiver.setActiveRowSet(null);
    }

    protected boolean isActiveBatch(RowBatchReceiverImpl batch) {
      return activeBatch == batch;
    }

    public int rowIndex() {
      return rowCount;
    }

    @Override
    public int rowCount() {
      return rowCount;
    }

  }

  public static class RowBatchReceiverImpl implements RowBatchReceiver
  {

    private RowSetReceiverImpl rowSet;

    public RowBatchReceiverImpl(RowSetReceiverImpl rowSetReceiver) {
      this.rowSet = rowSetReceiver;
    }

    @Override
    public RowBuilder row() {
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
    public void close() {
      if ( ! rowSet.isActiveBatch(this) ) {
        return;
      }

    }

    @Override
    public int rowCount() {
      // TODO Auto-generated method stub
      return 0;
    }

  }

  private enum State { START, ACTIVE, CLOSED; }
  private State state = State.START;
  private RowSetReceiverImpl activeRowSet;
  private final ScanState scanState;

  public ScanReceiverImpl() {
    scanState = new ScanState(this);
  }

  protected ScanState scanState() {
    return scanState;
  }

  @Override
  public RowSetBuilder newSchema() {
    return new RowSetBuilderImpl( this );
  }

  public boolean isActiveRowSet(RowSetReceiverImpl rowSet) {
    return activeRowSet == rowSet;
  }

  protected void setActiveRowSet( RowSetReceiverImpl rowSet ) {
    assertOpen( );
    if (activeRowSet == rowSet) {
      return;
    }
    if (activeRowSet != null) {
      activeRowSet.close();
    }
    activeRowSet = rowSet;
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
    activeRowSet.close( );
    activeRowSet = null;
    state = State.CLOSED;
  }

  @Override
  public RowSetReceiver rowSet() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int rowCount() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public RowSetBuilder reviseSchema() {
    // TODO Auto-generated method stub
    return null;
  }

}
