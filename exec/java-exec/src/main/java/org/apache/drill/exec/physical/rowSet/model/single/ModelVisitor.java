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

import org.apache.drill.exec.physical.rowSet.model.single.AbstractSingleTupleModel.AbstractSingleColumnModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.MapColumnModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.MapModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.PrimitiveColumnModel;

/**
 * Visitor for the single batch model.
 *
 * @param <R> Return value from the visitor
 * @param <A> Argument passed to the visitor
 */

public class ModelVisitor<R, A> {

    protected R visitRow(SingleRowSetModel row, A arg) {
      return visitTuple(row, arg);
    }

    protected R visitMap(MapModel map, A arg) {
      return visitTuple(map, arg);
    }

    protected R visitMapArray(MapModel map, A arg) {
      return visitTuple(map, arg);
    }

    protected R visitTuple(AbstractSingleTupleModel tuple, A arg) {
      return tuple.visitChildren(this, arg);
    }

    protected R visitPrimitiveColumn(PrimitiveColumnModel column, A arg) {
      return visitColumn(column, arg);
    }

    protected R visitPrimitiveArrayColumn(PrimitiveColumnModel column, A arg) {
      return visitColumn(column, arg);
    }

    protected R visitMapColumn(MapColumnModel column, A arg) {
      visitColumn(column, arg);
      return column.mapModelImpl().visit(this, arg);
    }

    protected R visitMapArrayColumn(MapColumnModel column, A arg) {
      visitColumn(column, arg);
      return column.mapModelImpl().visit(this, arg);
    }

//    protected void visitListColumn(T column) {
//      visitColumn(column);
//    }

    protected R visitColumn(AbstractSingleColumnModel column, A arg) { return null; }
  }