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
package org.apache.drill.test.rowSet.test;

import static org.apache.drill.test.rowSet.RowSetUtilities.intArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.convert.AbstractWriteConverter;
import org.apache.drill.exec.vector.accessor.convert.ColumnConversionFactory;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests the column type converter feature of the column metadata
 * and of the RowSetWriter.
 */

@Category(RowSetTests.class)
public class TestColumnConverter extends SubOperatorTest {

  public static final String CONVERTER_PROP = "test.conversion";
  public static final String CONVERT_TO_INT = "int";

  public static void setConverterProp(ColumnMetadata colSchema, String value) {
    colSchema.setProperty(CONVERTER_PROP, value);
  }

  /**
   * Simple type converter that allows string-to-int conversions.
   * Inherits usual int value support from the base writer.
   */
  public static class TestConverter extends AbstractWriteConverter {

    public TestConverter(ScalarWriter baseWriter) {
      super(baseWriter);
    }

    @Override
    public void setString(String value) {
      setInt(Integer.parseInt(value));
    }
  }

  /**
   * Mock conversion factory that uses a property on the column metadata
   * to indicate that a converter should be inserted.
   */
  public static class ConverterFactory implements ColumnConversionFactory {

    @Override
    public AbstractWriteConverter newWriter(ScalarWriter baseWriter) {
      String value = baseWriter.schema().property(CONVERTER_PROP);
      if (value == null) {
        return null;
      }
      if (value.equals(CONVERT_TO_INT)) {
        return new TestConverter(baseWriter);
      }
      return null;
    }
  }

  @Test
  public void testScalarConverter() {

    // Create the schema

    TupleMetadata schema = new SchemaBuilder()
        .add("n1", MinorType.INT)
        .addNullable("n2", MinorType.INT)
        .buildSchema();

    // Add a type converter. Passed in as a factory
    // since we must create a new one for each row set writer.

    setConverterProp(schema.metadata("n1"), CONVERT_TO_INT);
    setConverterProp(schema.metadata("n2"), CONVERT_TO_INT);

    // Write data as both a string as an integer

    ConverterFactory conversionFactory = new ConverterFactory();
    RowSet actual = new RowSetBuilder(fixture.allocator(), schema, conversionFactory)
        .addRow("123", "12")
        .addRow(234, 23)
        .build();

    // Build the expected vector without a type converter.

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("n1", MinorType.INT)
        .addNullable("n2", MinorType.INT)
        .buildSchema();
    final SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(123, 12)
        .addRow(234, 23)
        .build();

    // Compare

    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testArrayConverter() {

    // Create the schema

    TupleMetadata schema = new SchemaBuilder()
        .addArray("n", MinorType.INT)
        .buildSchema();

    // Add a type converter. Passed in as a factory
    // since we must create a new one for each row set writer.

    setConverterProp(schema.metadata("n"), CONVERT_TO_INT);

    // Write data as both a string as an integer

    ConverterFactory conversionFactory = new ConverterFactory();
    RowSet actual = new RowSetBuilder(fixture.allocator(), schema, conversionFactory)
        .addSingleCol(strArray("123", "124"))
        .addSingleCol(intArray(234, 235))
        .build();

    // Build the expected vector without a type converter.

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("n", MinorType.INT)
        .buildSchema();
    final SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addSingleCol(intArray(123, 124))
        .addSingleCol(intArray(234, 235))
        .build();

    // Compare

    RowSetUtilities.verify(expected, actual);
  }
}
