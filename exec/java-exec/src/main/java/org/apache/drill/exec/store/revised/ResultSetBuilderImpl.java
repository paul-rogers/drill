package org.apache.drill.exec.store.revised;

public class ResultSetBuilderImpl /* implements ResultSetBuilder */ {

//  public static class BufferedRowBuilderImpl implements RowBuilder {
//
//    private final Object rowBuffer[];
//
//    @Override
//    public ColumnWriter column(int index) {
//      // TODO Auto-generated method stub
//      return null;
//    }
//
//    @Override
//    public ColumnWriter column(String path) {
//      // TODO Auto-generated method stub
//      return null;
//    }
//
//    @Override
//    public void accept() {
//      // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public void reject() {
//      // Nothing to do, just let Java garbage collect the buffer
//    }
//  }
//
//  public static class DirectRowBuilderImpl implements RowBuilder {
//
//    @Override
//    public ColumnWriter column(int index) {
//      // TODO Auto-generated method stub
//      return null;
//    }
//
//    @Override
//    public ColumnWriter column(String path) {
//      // TODO Auto-generated method stub
//      return null;
//    }
//
//    @Override
//    public void accept() {
//      // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public void reject() {
//      // TODO Auto-generated method stub
//
//    }
//
//  }
//
//  public static class RowSetBuilderImpl implements RowSetBuilder {
//
//    @Override
//    public RowBuilder row() {
//      // TODO Auto-generated method stub
//      return null;
//    }
//
//    @Override
//    public boolean full() {
//      // TODO Auto-generated method stub
//      return false;
//    }
//
//    @Override
//    public void build() {
//      // TODO Auto-generated method stub
//
//    }
//
//  }
//
//  private enum State { START, NEW_SCHEMA, START_SET, LOAD, EOF };
//
//  private State state = State.START;
//  private RowSchema schema;
//
//  @Override
//  public SchemaBuilder newSchema() {
//    if (state == State.EOF) {
//      throw new IllegalStateException( "Already at EOF." );
//    }
//    if (state == State.START) {
//      flush( );
//      state = State.NEW_SCHEMA;
//    }
//    schema = null;
//    SchemaBuilderImpl builder = new SchemaBuilderImpl( );
//    builder.listener( new SomeListener( ) );
//    return builder;
//  }
//
//  @Override
//  public SchemaBuilder extendSchema() {
//    // TODO Auto-generated method stub
//    return null;
//  }
//
//  @Override
//  public SchemaBuilder reviseSchema() {
//    // TODO Auto-generated method stub
//    return null;
//  }
//
//  @Override
//  public RowSetBuilder rowSet() {
//    if (state == State.EOF) {
//      throw new IllegalStateException( "Already at EOF." );
//    }
//    if (schema == null) {
//      throw new IllegalStateException( "No schema provided" );
//    }
//    flush( );
//    return new RowSetBuilderImpl( this );
//  }
//
//  @Override
//  public void eof() {
//    flush( );
//    state = State.EOF;
//  }
//
//  private void flush() {
//
//  }

}
