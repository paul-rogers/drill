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
package org.apache.drill.exec.physical.impl.scan;

import org.apache.drill.exec.physical.rowSet.impl.TupleNameSpace;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;

/**
 * Similar to {@link BatchSchema}, except that
 * <ul>
 * <li>Provides fast lookup by name.</li>
 * <li>Does not carry a selection vector model.</li>
 * </ul>
 * This form us useful when performing semantic analysis.
 */

public class MaterializedSchema {
  public final TupleNameSpace<MaterializedField> nameSpace = new TupleNameSpace<>();

  public MaterializedSchema() { }

  public MaterializedSchema(BatchSchema schema) {
    for (MaterializedField field : schema) {
      add(field);
    }
  }

  public void add(MaterializedField field) {
    nameSpace.add(field.getName(), field);
  }

  public MaterializedField column(String name) {
    return nameSpace.get(name);
  }

  public int index(String name) {
    return nameSpace.indexOf(name);
  }

  public MaterializedField column(int index) {
    return nameSpace.get(index);
  }

  public int size() { return nameSpace.count(); }

  public boolean isEmpty() { return nameSpace.count( ) == 0; }

}