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
package org.apache.drill.test;

import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.test.TestRowSet.ColumnWriter;

public abstract class AbstractColumnWriter extends ColumnAccessor implements ColumnWriter {

  @Override
  public void setNull() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setInt(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setString(String value) {
    throw new UnsupportedOperationException();
  }

  public static class IntColumnWriter extends AbstractColumnWriter {

    private IntVector.Mutator mutator;

    @Override
    protected void bind(RowIndex rowIndex, ValueVector vector) {
      bind(rowIndex);
      this.mutator = ((IntVector) vector).getMutator();
    }

    @Override
    public void setInt(int value) {
      mutator.setSafe(rowIndex(), value);
    }
  }

  public static class VarCharColumnWriter extends AbstractColumnWriter {

    private VarCharVector.Mutator mutator;

    @Override
    protected void bind(RowIndex rowIndex, ValueVector vector) {
      bind(rowIndex);
      this.mutator = ((VarCharVector) vector).getMutator();
    }

    @Override
    public void setString(String value) {
      mutator.setSafe(rowIndex(), value.getBytes());
    }
  }

}