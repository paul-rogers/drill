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
package org.apache.drill.exec.physical.rowSet.impl;

import org.apache.drill.exec.physical.rowSet.model.single.AbstractSingleTupleModel.AbstractSingleColumnModel;
import org.apache.drill.exec.physical.rowSet.model.single.AbstractSingleTupleModel.ColumnCoordinator;

public abstract class ColumnState implements ColumnCoordinator {

  public static class MapColumnState extends ColumnState {

    public MapColumnState(ResultSetLoaderImpl resultSetLoader) {
      super(resultSetLoader);
    }

    @Override
    public void overflowed(AbstractSingleColumnModel model) {
      // Should never occur: map columns can't overflow
      // (only the content columns are subject to overflow.)

      throw new IllegalStateException();
    }
  }

  protected enum State {

    /**
     * Column is in the normal state of writing with no overflow
     * in effect.
     */

    NORMAL,

    /**
     * Like NORMAL, but means that the data has overflowed and the
     * column's data for the current row appears in the new,
     * overflow batch. For a client that omits some columns, written
     * columns will be in OVERFLOW state, unwritten columns in
     * NORMAL state.
     */

    OVERFLOW,

    /**
     * Indicates that the column has data saved
     * in the overflow batch.
     */

    LOOK_AHEAD,

    /**
     * Like LOOK_AHEAD, but indicates the special case that the column
     * was added after overflow, so there is no vector for the column
     * in the harvested batch.
     */

    NEW_LOOK_AHEAD
  }

  protected final ResultSetLoaderImpl resultSetLoader;
  protected final int addVersion;
  protected State state;

  public ColumnState(ResultSetLoaderImpl resultSetLoader) {
    this.resultSetLoader = resultSetLoader;
    this.addVersion = resultSetLoader.bumpVersion();
    state = resultSetLoader.hasOverflow() ?
        State.NEW_LOOK_AHEAD : State.NORMAL;
  }
}
