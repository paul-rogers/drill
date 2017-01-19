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
