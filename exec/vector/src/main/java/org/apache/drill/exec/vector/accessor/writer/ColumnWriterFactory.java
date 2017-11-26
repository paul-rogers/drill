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
package org.apache.drill.exec.vector.accessor.writer;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.ColumnMetadata;
import org.apache.drill.exec.vector.NullableVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnAccessorUtils;
import org.apache.drill.exec.vector.accessor.writer.AbstractArrayWriter.ArrayObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractScalarWriter.ScalarObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.dummy.DummyArrayWriter;
import org.apache.drill.exec.vector.accessor.writer.dummy.DummyScalarWriter;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;

/**
 * Gather generated writer classes into a set of class tables to allow rapid
 * run-time creation of writers. Builds the writer and its object writer
 * wrapper which binds the vector to the writer.
 * <p>
 * Compared to the reader factory, the writer factor is a bit more complex
 * as it must handle both the projected ("real" writer) and unprojected
 * ("dummy" writer) cases. Because of the way the various classes interact,
 * it is cleaner to put the factory methods here rather than in the various
 * writers, as is done in the case of the readers.
 */

@SuppressWarnings("unchecked")
public class ColumnWriterFactory {

  private static final int typeCount = MinorType.values().length;
  private static final Class<? extends BaseScalarWriter> requiredWriters[] = new Class[typeCount];

  static {
    ColumnAccessorUtils.defineRequiredWriters(requiredWriters);
  }

  public static AbstractObjectWriter buildColumnWriter(ColumnMetadata schema, ValueVector vector) {
    if (vector == null) {
      return buildDummyColumnWriter(schema);
    }

    // Build a writer for a materialized column.

    assert schema.type() == vector.getField().getType().getMinorType();
    assert schema.mode() == vector.getField().getType().getMode();

    switch (schema.type()) {
    case GENERIC_OBJECT:
    case LATE:
    case NULL:
    case LIST:
    case MAP:
    case UNION:
      throw new UnsupportedOperationException(schema.type().toString());
    default:
      switch (schema.mode()) {
      case OPTIONAL:
        return buildNullable(schema, (NullableVector) vector);
      case REQUIRED:
        return new ScalarObjectWriter(schema, newWriter(vector));
      case REPEATED:
        RepeatedValueVector repeatedVector = (RepeatedValueVector) vector;
        return ScalarArrayWriter.build(schema, repeatedVector,
                newWriter(repeatedVector.getDataVector()));
      default:
        throw new UnsupportedOperationException(schema.mode().toString());
      }
    }
  }

  private static AbstractObjectWriter buildNullable(ColumnMetadata schema,
      NullableVector vector) {
    NullableVector nullableVector = vector;
    return NullableScalarWriter.build(schema, nullableVector,
            newWriter(nullableVector.getValuesVector()));
  }

  /**
   * Build a writer for a non-projected column.
   * @param schema schema of the column
   * @return a "dummy" writer for the column
   */

  public static AbstractObjectWriter buildDummyColumnWriter(ColumnMetadata schema) {
    switch (schema.type()) {
    case GENERIC_OBJECT:
    case LATE:
    case NULL:
    case LIST:
    case MAP:
    case UNION:
      throw new UnsupportedOperationException(schema.type().toString());
    default:
      ScalarObjectWriter scalarWriter = new ScalarObjectWriter(schema,
          new DummyScalarWriter());
      switch (schema.mode()) {
      case OPTIONAL:
      case REQUIRED:
        return scalarWriter;
      case REPEATED:
        return new ArrayObjectWriter(schema,
            new DummyArrayWriter(
              scalarWriter));
      default:
        throw new UnsupportedOperationException(schema.mode().toString());
      }
    }
  }
  public static BaseScalarWriter newWriter(ValueVector vector) {
    MajorType major = vector.getField().getType();
    MinorType type = major.getMinorType();
    try {
      Class<? extends BaseScalarWriter> accessorClass = requiredWriters[type.ordinal()];
      if (accessorClass == null) {
        throw new UnsupportedOperationException(type.toString());
      }
      Constructor<? extends BaseScalarWriter> ctor = accessorClass.getConstructor(ValueVector.class);
      return ctor.newInstance(vector);
    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException |
             SecurityException | IllegalArgumentException | InvocationTargetException e) {
      throw new IllegalStateException(e);
    }
  }
}
