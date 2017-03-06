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

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;

public class TestSchema {

  public static class TestSchemaBuilder {
    private List<MaterializedField> columns = new ArrayList<>( );

    public TestSchemaBuilder() {

    }

    public TestSchemaBuilder(TestSchema base) {
      columns.addAll(base.columns);
    }

    public TestSchemaBuilder add(String pathName, MajorType type) {
      MaterializedField col = MaterializedField.create(pathName, type);
      columns.add(col);
      return this;
    }

    public TestSchemaBuilder add(String pathName, MinorType type, DataMode mode) {
      return add(pathName, MajorType.newBuilder()
          .setMinorType(type)
          .setMode(mode)
          .build()
          );
    }

    public TestSchemaBuilder add(String pathName, MinorType type) {
      return add(pathName, type, DataMode.REQUIRED);
    }

    public TestSchemaBuilder addNullable(String pathName, MinorType type) {
      return add(pathName, type, DataMode.OPTIONAL);
    }

    public TestSchema build() {
      return new TestSchema(columns);
    }

  }

  private List<MaterializedField> columns = new ArrayList<>( );

  public TestSchema(List<MaterializedField> columns) {
    this.columns.addAll( columns );
  }

  public int count( ) { return columns.size(); }
  public MaterializedField get(int i) { return columns.get(i); }

  public static TestSchemaBuilder builder( ) {
    return new TestSchemaBuilder();
  }

  public TestSchemaBuilder revise() {
    return new TestSchemaBuilder(this);
  }

}