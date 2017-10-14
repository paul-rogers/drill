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
package org.apache.drill.exec.physical.impl.scan.project;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.record.MaterializedField;

public interface ColumnProjection {

  public static final int WILDCARD = 1;
  public static final int UNRESOLVED = 2;
  public static final int NULL = 5;
  public static final int PROJECTED = 6;
  public static final int CONTINUED = 7;

  String name();
  boolean resolved();
  int nodeType();

  public static abstract class BaseProjection implements ColumnProjection {

    private int id;
    private boolean resolved;

    public BaseProjection(int id, boolean resolved) {
      this.id = id;
      this.resolved = resolved;
    }

    @Override
    public boolean resolved() { return resolved; }

    @Override
    public int nodeType() { return id; }
  }

  public class NamedColumn extends BaseProjection {

    private final String name;

    public NamedColumn(String name, int id, boolean resolved) {
      super(id, resolved);
      this.name = name;
    }

    @Override
    public String name() { return name; }
  }

  public class TypedColumn extends BaseProjection {

    private final MaterializedField schema;

    public TypedColumn(String name, MajorType type, int id, boolean resolved) {
      this(MaterializedField.create(name, type), id, resolved);
    }

    public TypedColumn(MaterializedField schema, int id, boolean resolved) {
      super(id, resolved);
      this.schema = schema;
    }

    @Override
    public String name() { return schema.getName(); }

    public MajorType type() { return schema.getType(); }

    public MaterializedField schema() { return schema; }

    public static TypedColumn newContinued(MaterializedField schema) {
      return new TypedColumn(schema, TypedColumn.CONTINUED, false);
    }
  }

}