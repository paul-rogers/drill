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
package org.apache.drill.exec.vector.accessor2.impl;

import java.math.BigDecimal;

import org.apache.drill.exec.vector.VectorOverflowException;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor2.ArrayWriter;
import org.apache.drill.exec.vector.accessor2.ObjectWriter.ObjectType;
import org.apache.drill.exec.vector.accessor2.ScalarWriter;
import org.apache.drill.exec.vector.accessor2.TupleWriter;
import org.apache.drill.exec.vector.accessor2.impl.AbstractScalarWriter.ScalarObjectWriter;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;
import org.joda.time.Period;

/**
 * Writer for a column that holds an array of scalars. This writer manages
 * the array itself. A type-specific child writer manages the elements within
 * the array. The overall row index (usually) provides the index into
 * the offset vector. An array-specific element index provides the index
 * into elements.
 * <p>
 * This class manages the offset vector directly. Doing so saves one read and
 * one write to direct memory per element value.
 * <p>
 * Provides generic write methods for testing and other times when
 * convenience is more important than speed.
 */

public class ScalarArrayWriterImpl extends AbstractArrayWriterImpl {

  private final BaseElementWriter elementWriter;

  public ScalarArrayWriterImpl(ColumnWriterIndex baseIndex, RepeatedValueVector vector, BaseElementWriter elementWriter) {
    super(baseIndex, vector, new ScalarObjectWriter(elementWriter));
    this.elementWriter = elementWriter;
    elementWriter.bind(elementIndex(), vector.getDataVector());
  }

  @Override
  public void set(Object... values) throws VectorOverflowException {
    for (Object value : values) {
      entry().set(value);
    }
  }

  @Override
  public void setArray(Object array) throws VectorOverflowException {
  if (array == null) {
      // Assume null means a 0-element array since Drill does
      // not support null for the whole array.

      return;
    }
    if (! array.getClass().getName().startsWith("[")) {
      throw new IllegalArgumentException("Argument must be an array");
    }
    String objClass = array.getClass().getName();
    if (!objClass.startsWith("[")) {
      throw new IllegalArgumentException("Argument is not an array");
    }

    // Figure out type

    char second = objClass.charAt( 1 );
    switch ( second ) {
    case  'B':
      setByteArray((byte[]) array );
      break;
    case  'S':
      setShortArray((short[]) array );
      break;
    case  'I':
      setIntArray((int[]) array );
      break;
    case  'J':
      setLongArray((long[]) array );
      break;
    case  'F':
      setFloatArray((float[]) array );
      break;
    case  'D':
      setDoubleArray((double[]) array );
      break;
    case  'Z':
      setBooleanArray((boolean[]) array );
      break;
    case 'L':
      int posn = objClass.indexOf(';');

      // If the array is of type Object, then we have no type info.

      String memberClassName = objClass.substring( 2, posn );
      if (memberClassName.equals(String.class.getName())) {
        setStringArray((String[]) array );
      } else if (memberClassName.equals(Period.class.getName())) {
        setPeriodArray((Period[]) array );
      } else if (memberClassName.equals(BigDecimal.class.getName())) {
        setBigDecimalArray((BigDecimal[]) array );
      } else {
        throw new IllegalArgumentException( "Unknown Java array type: " + memberClassName );
      }
      break;
    default:
      throw new IllegalArgumentException( "Unknown Java array type: " + second );
    }
  }

  public void setBooleanArray(boolean[] value) throws VectorOverflowException {
    for (int i = 0; i < value.length; i++) {
      elementWriter.setInt(value[i] ? 1 : 0);
    }
  }

  public void setByteArray(byte[] value) throws VectorOverflowException {
    for (int i = 0; i < value.length; i++) {
      elementWriter.setInt(value[i]);
    }
  }

  public void setShortArray(short[] value) throws VectorOverflowException {
    for (int i = 0; i < value.length; i++) {
      elementWriter.setInt(value[i]);
    }
  }

  public void setIntArray(int[] value) throws VectorOverflowException {
    for (int i = 0; i < value.length; i++) {
      elementWriter.setInt(value[i]);
    }
  }

  public void setLongArray(long[] value) throws VectorOverflowException {
    for (int i = 0; i < value.length; i++) {
      elementWriter.setLong(value[i]);
    }
  }

  public void setFloatArray(float[] value) throws VectorOverflowException {
    for (int i = 0; i < value.length; i++) {
      elementWriter.setDouble(value[i]);
    }
  }

  public void setDoubleArray(double[] value) throws VectorOverflowException {
    for (int i = 0; i < value.length; i++) {
      elementWriter.setDouble(value[i]);
    }
  }

  public void setStringArray(String[] value) throws VectorOverflowException {
    for (int i = 0; i < value.length; i++) {
      elementWriter.setString(value[i]);
    }
  }

  public void setPeriodArray(Period[] value) throws VectorOverflowException {
    for (int i = 0; i < value.length; i++) {
      elementWriter.setPeriod(value[i]);
    }
  }

  public void setBigDecimalArray(BigDecimal[] value) throws VectorOverflowException {
    for (int i = 0; i < value.length; i++) {
      elementWriter.setDecimal(value[i]);
    }
  }
}
