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
import org.apache.drill.exec.physical.impl.scan.project.RowBatchMerger.ProjectionSet;
import org.apache.drill.exec.physical.rowSet.impl.ResultVectorCacheImpl;
import org.apache.drill.exec.record.VectorContainer;

public class NullColumnManager {


  /**
   * Creates null columns if needed.
   */

  private NullColumnLoader nullColumnLoader;
  private final ResultVectorCacheImpl vectorCache;

  /**
   * The reader-specified null type if other than the default.
   */

  private final MajorType nullType;

  public NullColumnManager(ResultVectorCacheImpl vectorCache,
      MajorType nullType) {
    this.vectorCache = vectorCache;
    this.nullType = nullType;
  }

  public ProjectionSet buildProjection(ProjectionLifecycle projectionDefn) {
    close();
    TableLevelProjection tableProj = projectionDefn.tableProjection();
    if (! tableProj.hasNullColumns()) {
      return null;
    }

    nullColumnLoader = new NullColumnLoader(vectorCache, tableProj.nullColumns(), nullType);

    // Map null columns from the null column loader schema into the output
    // schema.

    VectorContainer nullsContainer = nullColumnLoader.output();
    ProjectionSet projSet = new ProjectionSet(nullsContainer);
    for (int i = 0; i < tableProj.nullColumns().size(); i++) {
      int projIndex = tableProj.nullProjectionMap()[i];
      projSet.addDirectProjection(i, projIndex);
    }
    return projSet;
  }

  public void load(int rowCount) {
    if (nullColumnLoader != null) {
      nullColumnLoader.load(rowCount);
    }
  }

  public void close() {
    if (nullColumnLoader != null) {
      nullColumnLoader.close();
      nullColumnLoader = null;
    }
  }

}
