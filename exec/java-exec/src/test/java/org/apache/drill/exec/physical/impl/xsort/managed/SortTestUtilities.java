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
package org.apache.drill.exec.physical.impl.xsort.managed;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.test.rowSet.RowSetSchema;

public class SortTestUtilities {

  private SortTestUtilities() { }

  public static RowSetSchema makeSchema(MinorType type, boolean nullable) {
    return RowSetSchema.builder()
        .add("key", type, nullable ? DataMode.OPTIONAL : DataMode.REQUIRED)
        .add("value", MinorType.VARCHAR)
        .build();
  }

  public static RowSetSchema nonNullSchema() {
    return makeSchema(MinorType.INT, false);
  }

  public static RowSetSchema nullSchema() {
    return makeSchema(MinorType.INT, true);
  }

}
