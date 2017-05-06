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

import java.math.BigDecimal;

import org.apache.drill.exec.physical.rowSet.ArrayLoader;
import org.apache.drill.exec.physical.rowSet.ColumnLoader;
import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VectorOverflowException;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.impl.AbstractColumnWriter;
import org.apache.drill.exec.vector.accessor.impl.ColumnAccessorFactory;
import org.joda.time.Period;

public class ColumnLoaderImpl implements ColumnLoader {

  private static final String ROLLOVER_FAILED = "Row batch rollover failed.";

  private final AbstractColumnWriter writer;

  protected ColumnLoaderImpl(ColumnWriterIndex index, ValueVector vector) {
    writer = ColumnAccessorFactory.newWriter(vector.getField().getType());
    writer.bind(index, vector);
  }

  public int writeIndex() { return writer.lastWriteIndex(); }
  public void reset() { writer.reset(); }
  public void resetTo(int dest) { writer.reset(dest); }

  @Override
  public void setInt(int value) {
    try {
      writer.setInt(value);
    } catch (VectorOverflowException e) {
      try {
        writer.setInt(value);
      } catch (VectorOverflowException e1) {
        throw new IllegalStateException(ROLLOVER_FAILED);
      }
    }
  }

  @Override
  public void setLong(long value) {
    // TODO Auto-generated method stub
  }

  @Override
  public void setDouble(double value) {
    // TODO Auto-generated method stub
  }

  @Override
  public void setString(String value) {
    try {
      writer.setString(value);
    } catch (VectorOverflowException e) {
      try {
        writer.setString(value);
      } catch (VectorOverflowException e1) {
        throw new IllegalStateException(ROLLOVER_FAILED);
      }
    }
  }

  @Override
  public void setBytes(byte[] value) {
    try {
      writer.setBytes(value);
    } catch (VectorOverflowException e) {
      try {
        writer.setBytes(value);
      } catch (VectorOverflowException e1) {
        throw new IllegalStateException(ROLLOVER_FAILED);
      }
    }
  }

  @Override
  public void setDecimal(BigDecimal value) {
    // TODO Auto-generated method stub
  }

  @Override
  public void setPeriod(Period value) {
    // TODO Auto-generated method stub
  }

  @Override
  public TupleLoader map() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ArrayLoader array() {
    // TODO Auto-generated method stub
    return null;
  }

}
