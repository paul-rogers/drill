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

import org.apache.drill.exec.vector.accessor.AbstractColumnAccessor.RowIndex;
import org.apache.drill.test.rowSet.RowSet.RowSetAccessor;
import org.apache.drill.test.rowSet.RowSetSchema.AccessSchema;

public abstract class AbstractRowSetAccessor implements RowSetAccessor {

  public static abstract class AbstractRowIndex implements RowIndex {
    protected int rowIndex = -1;

    public int position() { return rowIndex; }
    public abstract boolean next();
    public abstract int size();
    public abstract boolean valid();
    public void setRowCount() { }
  }

  public static abstract class BoundedRowIndex extends AbstractRowIndex {

    protected final int rowCount;

    public BoundedRowIndex(int rowCount) {
      this.rowCount = rowCount;
    }

    @Override
    public boolean next() {
      if (++rowIndex < rowCount ) {
        return true;
      } else {
        rowIndex--;
        return false;
      }
    }

    @Override
    public int size() { return rowCount; }

    @Override
    public boolean valid() { return rowIndex < rowCount; }
  }

  protected final AccessSchema schema;
  protected final AbstractRowIndex index;

  protected AbstractRowSetAccessor(AbstractRowIndex index, AccessSchema schema) {
    this.index = index;
    this.schema = schema;
  }

  @Override
  public boolean next() { return index.next(); }

  @Override
  public boolean valid() { return index.valid(); }

  @Override
  public int index() { return index.position(); }

  @Override
  public int size() { return index.size(); }

  @Override
  public int rowIndex() { return index.index(); }

  @Override
  public int batchIndex() { return index.batch(); }

  @Override
  public AccessSchema schema() { return schema; }
}
