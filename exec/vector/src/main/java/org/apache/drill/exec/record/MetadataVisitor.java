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
package org.apache.drill.exec.record;

import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;

/**
 * Walks the metadata tree applying an operation, defined by the subclass,
 * to each metadata node (tuple or column.)
 *
 * @param <R> return value of each visit method
 * @param <A> argument passed to each visit method
 */

public class MetadataVisitor<R, A> {

  protected R applyToRow(TupleMetadata row, A arg) {
    return visitRow(row, arg);
  }

  protected R applyToMap(TupleMetadata row, A arg) {
    return visitMap(row, arg);
  }

  protected R visitRow(TupleMetadata row, A arg) {
    return visitTuple(row, arg);
  }

  protected R visitMap(TupleMetadata map, A arg) {
    return visitTuple(map, arg);
  }

  protected R visitTuple(TupleMetadata tuple, A arg) {
    return visitChildren(tuple, arg);
  }

  protected R visitChildren(TupleMetadata tuple, A arg) {
    for (int i = 0; i < tuple.size(); i++) {
      apply(tuple.metadata(i), arg);
    }
    return null;
  }

  public R apply(ColumnMetadata col, A arg) {
    if (col.isMap()) {
      return visitMapColumn(col, arg);
    } else {
      return visitPrimitiveColumn(col, arg);
    }
  }

  protected R visitPrimitiveColumn(ColumnMetadata row, A arg) {
    return visitColumn(row, arg);
  }


  protected R visitMapColumn(ColumnMetadata row, A arg) {
    return visitColumn(row, arg);
  }

  private R visitColumn(ColumnMetadata row, A arg) {
    return null;
  }
}
