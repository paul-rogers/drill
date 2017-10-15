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

import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.rowSet.impl.OptionBuilder;
import org.apache.drill.exec.record.TupleMetadata;

/**
 * Handle schema mapping for early-schema tables: the schema is
 * known before the first batch is read and stays constant for the
 * entire table. The schema can be used to populate the batch
 * loader.
 */

class EarlySchemaDriver extends TableSchemaDriver {

  private final TupleMetadata tableSchema;

  public EarlySchemaDriver(ScanProjector scanProjector, TupleMetadata tableSchema) {
    super(scanProjector);
    this.tableSchema = tableSchema;
  }

  @Override
  protected void setupProjection(OptionBuilder options) {
    scanProjector.projectionDefn.startSchema(tableSchema);

    // Set up a selection list if available and is a subset of
    // table columns. (If we want all columns, either because of *
    // or we selected them all, then no need to add filtering.)

    if (! scanProjector.projectionDefn.scanProjection().projectAll()) {
      List<SchemaPath> projection = scanProjector.projectionDefn.tableProjection().projectedTableColumns();
      if (projection.size() < tableSchema.size()) {
        options.setProjection(projection);
      }
    }
  }

  @Override
  protected void setupSchema(OptionBuilder options) {

    // We know the table schema. Preload it into the
    // result set loader.

    options.setSchema(tableSchema);
  }

  @Override
  protected void setupProjection() {
    scanProjector.planProjection();

    // Set the output container to zero rows. Required so that we can
    // send the schema downstream in the form of an empty batch.

    scanProjector.output.getOutput().setRecordCount(0);
  }
}