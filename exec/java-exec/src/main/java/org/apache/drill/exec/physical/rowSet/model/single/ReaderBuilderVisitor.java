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
package org.apache.drill.exec.physical.rowSet.model.single;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.MapColumnModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.PrimitiveColumnModel;
import org.apache.drill.exec.vector.accessor.impl.ColumnAccessorFactory;
import org.apache.drill.exec.vector.accessor.reader.AbstractObjectReader;
import org.apache.drill.exec.vector.accessor.reader.MapReader;
import org.apache.drill.exec.vector.accessor.reader.ObjectArrayReader;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;

/**
 * Build a set of readers for a simple (non-hyper) result set.
 */

public abstract class ReaderBuilderVisitor extends ModelVisitor<Void, ReaderBuilderVisitor.Context> {

  public static class Context {
    protected MapColumnModel mapColumn;
    protected AbstractObjectReader mapReader;
    protected List<AbstractObjectReader> childReaders;
  }

  protected AbstractObjectReader[] buildTuple(AbstractSingleTupleModel tuple) {
    Context context = new Context();
    context.childReaders = new ArrayList<>();
    tuple.visitChildren(this, context);
    AbstractObjectReader readers[] = new AbstractObjectReader[context.childReaders.size()];
    return context.childReaders.toArray(readers);
  }

  @Override
  protected Void visitPrimitiveColumn(PrimitiveColumnModel column, Context context ) {
    return visitScalarColumn(column, context);
  }

  @Override
  protected Void visitPrimitiveArrayColumn(PrimitiveColumnModel column, Context context ) {
    return visitScalarColumn(column, context);
  }

  private Void visitScalarColumn(PrimitiveColumnModel column, Context context) {
    context.childReaders.add(ColumnAccessorFactory.buildColumnReader(column.vector()));
    return null;
  }

  @Override
  protected Void visitMapColumn(MapColumnModel column, Context context ) {
    context.childReaders.add(buildMap(column));
    return null;
  }

  @Override
  protected Void visitMapArrayColumn(MapColumnModel column, Context context ) {
    context.childReaders.add(ObjectArrayReader.build((RepeatedMapVector) column.vector(), buildMap(column)));
    return null;
  }

  protected AbstractObjectReader buildMap(MapColumnModel column) {
    return MapReader.build(column.schema(), buildTuple(column.mapModelImpl()));
  }
}
