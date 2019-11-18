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
package org.apache.drill.exec.store.mock;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.ColumnMetadata;

/**
 * Defines a column for the "enhanced" version of the mock data
 * source. This class is built from the column definitions in either
 * the physical plan or an SQL statement (which gives rise to a
 * physical plan.)
 */

public class ColumnDef {
  protected ColumnMetadata mockCol;
  protected String name;
  protected FieldGen generator;
  protected boolean nullable;
  protected int nullablePercent;

  public ColumnDef(ColumnMetadata mockCol) {
    this.mockCol = mockCol;
    name = mockCol.name();
    nullable = mockCol.isNullable();
    if (nullable) {
      nullablePercent = mockCol.intProperty(MockTableDef.NULL_RATE_PROP, 25);
    }
    makeGenerator();
  }

  /**
   * Create the data generator class for this column. The generator is
   * created to match the data type by default. Or, the plan can
   * specify a generator class (in which case the plan must ensure that
   * the generator produces the correct value for the column data type.)
   * The generator names a class: either a fully qualified name, or a
   * class in this package.
   */

  private void makeGenerator() {
    String genName = mockCol.property(MockTableDef.GENERATOR_PROP);
    if (genName != null) {
      if (! genName.contains(".")) {
        genName = "org.apache.drill.exec.store.mock." + genName;
      }
      try {
        ClassLoader cl = getClass().getClassLoader();
        Class<?> genClass = cl.loadClass(genName);
        generator = (FieldGen) genClass.newInstance();
      } catch (ClassNotFoundException | InstantiationException
          | IllegalAccessException | ClassCastException e) {
        throw new IllegalArgumentException(
            String.format("Generator %s is undefined for mock field 5s",
                genName, name));
      }
      return;
    }

    makeDefaultGenerator();
  }

  public ColumnDef(ColumnMetadata mockCol, int rep) {
    this(mockCol);
    name += Integer.toString(rep);
  }

  private void makeDefaultGenerator() {

    MinorType minorType = mockCol.type();
    switch (minorType) {
    case BIGINT:
      break;
    case BIT:
      generator = new BooleanGen();
      break;
    case DATE:
      break;
    case DECIMAL18:
      break;
    case DECIMAL28DENSE:
      break;
    case DECIMAL28SPARSE:
      break;
    case DECIMAL38DENSE:
      break;
    case DECIMAL38SPARSE:
      break;
    case VARDECIMAL:
      break;
    case DECIMAL9:
      break;
    case FIXED16CHAR:
      break;
    case FIXEDBINARY:
      break;
    case FIXEDCHAR:
      break;
    case FLOAT4:
      break;
    case FLOAT8:
      generator = new DoubleGen();
      break;
    case GENERIC_OBJECT:
      break;
    case INT:
      generator = new IntGen();
      break;
    case INTERVAL:
      break;
    case INTERVALDAY:
      break;
    case INTERVALYEAR:
      break;
    case LATE:
      break;
    case LIST:
      break;
    case MAP:
      break;
    case MONEY:
      break;
    case NULL:
      break;
    case SMALLINT:
      break;
    case TIME:
      break;
    case TIMESTAMP:
      break;
    case TIMESTAMPTZ:
      break;
    case TIMETZ:
      break;
    case TINYINT:
      break;
    case UINT1:
      break;
    case UINT2:
      break;
    case UINT4:
      break;
    case UINT8:
      break;
    case UNION:
      break;
    case VAR16CHAR:
      break;
    case VARBINARY:
      break;
    case VARCHAR:
      generator = new StringGen();
      break;
    default:
      break;
    }
    if (generator == null) {
      throw new IllegalArgumentException(
          String.format("No default column generator for column %s of type %s",
              mockCol.name(), minorType));
    }
  }

  public ColumnMetadata getConfig() { return mockCol; }
  public String getName() { return name; }
}
