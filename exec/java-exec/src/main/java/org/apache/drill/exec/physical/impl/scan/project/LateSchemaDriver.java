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

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.rowSet.impl.OptionBuilder;

/**
 * Handle schema mapping for a late-schema table. The schema is not
 * known until the first batch is read, and may change after any
 * batch. All we know up front is the list of columns (if any)
 * that the query projects. But, we don't know their types.
 */

class LateSchemaDriver extends TableSchemaDriver {

  /**
   * Tracks the schema version last seen from the table loader. Used to detect
   * when the reader changes the table loader schema.
   */

  private int prevTableSchemaVersion = -1;

  LateSchemaDriver(ScanProjector scanProjector) {
    super(scanProjector);
  }

  @Override
  protected void setupProjection(OptionBuilder options) {

    // Set up a selection list if available. Since the actual columns are
    // built on the fly, we need to set up the selection ahead of time and
    // can't optimize for the "selected all the columns" case.

    if (! scanProjector.projectionDefn.scanProjection().projectAll()) {
      // Temporary: convert names to paths. Need to handle full paths
      // throughout.

      List<SchemaPath> paths = new ArrayList<>();
      for (ColumnProjection col : scanProjector.projectionDefn.scanProjection().outputCols()) {
        if (col.nodeType() == UnresolvedColumn.UNRESOLVED) {
          paths.add(((UnresolvedColumn) col).source());
        }
      }
      options.setProjection(paths);
    }
  }

  @Override
  public int endOfBatch() {
    int count = super.endOfBatch();
    if (prevTableSchemaVersion < tableLoader.schemaVersion()) {
      scanProjector.projectionDefn.startSchema(tableLoader.writer().schema());
      scanProjector.planProjection();
      prevTableSchemaVersion = tableLoader.schemaVersion();
    }
    return count;
  }
}