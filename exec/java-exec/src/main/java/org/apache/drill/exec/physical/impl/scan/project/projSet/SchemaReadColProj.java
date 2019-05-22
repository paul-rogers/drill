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
package org.apache.drill.exec.physical.impl.scan.project.projSet;

import org.apache.drill.exec.physical.rowSet.project.RequestedTuple.RequestedColumn;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.convert.ColumnConversionFactory;

/**
 * Projected column backed by a provided column schema and optionally
 * a projected column request.
 * Both the explicit projection and the provided schema constrain the
 * reader column types allowed. The provided schema may trigger a
 * type conversion.
 */

public class SchemaReadColProj extends BaseReadColProj {

  private final ColumnConversionFactory conversionFactory;

  public SchemaReadColProj(ColumnMetadata readSchema, RequestedColumn reqCol,
      ColumnMetadata outputSchema, ColumnConversionFactory conversionFactory) {
    super(readSchema, reqCol, outputSchema);
    this.conversionFactory = conversionFactory;
  }

  @Override
  public ColumnConversionFactory conversionFactory() { return conversionFactory; }
}
