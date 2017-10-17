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

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.record.MaterializedField;

public class ContinuedColumn implements ColumnProjection {

  public static final int ID = 7;

  private final MaterializedField schema;
  private final SchemaPath source;

  public ContinuedColumn(MaterializedField schema, SchemaPath source) {
    this.schema = schema;
    this.source = source;
  }

  @Override
  public String name() { return schema.getName(); }

  @Override
  public boolean resolved() { return false; }

  @Override
  public int nodeType() { return ID; }

  public MajorType type() { return schema.getType(); }

  @Override
  public SchemaPath source() { return source; }
}
