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

import org.apache.drill.exec.vector.accessor2.ArrayWriter;
import org.apache.drill.exec.vector.accessor2.Exp3;
import org.apache.drill.exec.vector.accessor2.ScalarWriter;
import org.joda.time.Period;

public abstract class AbstractArrayWriter implements ArrayWriter {

  @Override
  public void set(Object... values) {
    for (Object value : values) {
      entry().set(value);
    }
  }

  @Override
  public void setArray(Object array) {
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

  public void setBooleanArray(boolean[] value) {
    ScalarWriter scalarWriter = entry().scalar();
    for (int i = 0; i < value.length; i++) {
      scalarWriter.setInt(value[i] ? 1 : 0);
    }
  }

  public void setByteArray(byte[] value) {
    ScalarWriter scalarWriter = entry().scalar();
    for (int i = 0; i < value.length; i++) {
      scalarWriter.setInt(value[i]);
    }
  }

  public void setShortArray(short[] value) {
    ScalarWriter scalarWriter = entry().scalar();
    for (int i = 0; i < value.length; i++) {
      scalarWriter.setInt(value[i]);
    }
  }

  public void setIntArray(int[] value) {
    ScalarWriter scalarWriter = entry().scalar();
    for (int i = 0; i < value.length; i++) {
      scalarWriter.setInt(value[i]);
    }
  }

  public void setLongArray(long[] value) {
    ScalarWriter scalarWriter = entry().scalar();
    for (int i = 0; i < value.length; i++) {
      scalarWriter.setLong(value[i]);
    }
  }

  public void setFloatArray(float[] value) {
    ScalarWriter scalarWriter = entry().scalar();
    for (int i = 0; i < value.length; i++) {
      scalarWriter.setDouble(value[i]);
    }
  }

  public void setDoubleArray(double[] value) {
    ScalarWriter scalarWriter = entry().scalar();
    for (int i = 0; i < value.length; i++) {
      scalarWriter.setDouble(value[i]);
    }
  }

  public void setStringArray(String[] value) {
    ScalarWriter scalarWriter = entry().scalar();
    for (int i = 0; i < value.length; i++) {
      scalarWriter.setString(value[i]);
    }
  }

  public void setPeriodArray(Period[] value) {
    ScalarWriter scalarWriter = entry().scalar();
    for (int i = 0; i < value.length; i++) {
      scalarWriter.setPeriod(value[i]);
    }
  }

  public void setBigDecimalArray(BigDecimal[] value) {
    ScalarWriter scalarWriter = entry().scalar();
    for (int i = 0; i < value.length; i++) {
      scalarWriter.setDecimal(value[i]);
    }
  }
}
