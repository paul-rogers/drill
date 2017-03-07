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
package org.apache.drill.exec.vector.accessor;

public abstract class AbstractColumnReader extends ColumnAccessor implements ColumnReader {

  @Override
  public boolean isNull() {
    return false;
  }

  @Override
  public int getInt() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong() {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getString() {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBytes() {
    throw new UnsupportedOperationException();
  }
//  public static class IntColumnReader extends AbstractColumnReader {
//
//    private IntVector.Accessor accessor;
//
//    @Override
//    public void bind(RowIndex rowIndex, ValueVector vector) {
//      bind(rowIndex);
//      this.accessor = ((IntVector) vector).getAccessor();
//    }
//
//    @Override
//    public int getInt() {
//      return accessor.get(rowIndex());
//    }
//  }
//
//  public static class VarCharColumnReader extends AbstractColumnReader {
//
//    private VarCharVector.Accessor accessor;
//
//    @Override
//    public void bind(RowIndex rowIndex, ValueVector vector) {
//      bind(rowIndex);
//      this.accessor = ((VarCharVector) vector).getAccessor();
//    }
//
//    @Override
//    public String getString() {
//      return new String(accessor.get(rowIndex()), Charsets.UTF_8);
//    }
//  }

}