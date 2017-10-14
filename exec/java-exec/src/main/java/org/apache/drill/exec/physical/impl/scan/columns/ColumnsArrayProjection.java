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
package org.apache.drill.exec.physical.impl.scan.columns;

import org.apache.drill.common.types.TypeProtos.MajorType;

public class ColumnsArrayProjection {

  public static final String COLUMNS_COL = "columns";

  private final boolean columnsIndexes[];
  private final MajorType columnsArrayType;
  private final boolean hasColumnsArray;

  public ColumnsArrayProjection(ColumnsArrayParser builder) {
    hasColumnsArray = builder.columnsArrayCol != null;
    columnsArrayType = builder.columnsArrayType;
    if (builder.columnsIndexes == null) {
      columnsIndexes = null;
    } else {
      columnsIndexes = new boolean[builder.maxIndex];
      for (int i = 0; i < builder.columnsIndexes.size(); i++) {
        columnsIndexes[builder.columnsIndexes.get(i)] = true;
      }
    }
  }

  public boolean[] columnsArrayIndexes() { return columnsIndexes; }

  public MajorType columnsArrayType() { return columnsArrayType; }

  public boolean hasColumnsArray() { return hasColumnsArray; }
}