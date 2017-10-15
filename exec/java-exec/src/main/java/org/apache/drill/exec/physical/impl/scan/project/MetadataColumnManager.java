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

import org.apache.drill.exec.physical.impl.scan.file.FileLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.RowBatchMerger.ProjectionSet;
import org.apache.drill.exec.physical.rowSet.impl.ResultVectorCacheImpl;
import org.apache.drill.exec.record.VectorContainer;

public class MetadataColumnManager {

  private ResultVectorCacheImpl vectorCache;
  private ConstantColumnLoader metadataColumnLoader;

  public MetadataColumnManager(ResultVectorCacheImpl vectorCache) {
    this.vectorCache = vectorCache;
  }

  public void buildColumns(FileLevelProjection fileProjection) {
    if (! fileProjection.hasMetadata()) {
      return;
    }
    metadataColumnLoader = new ConstantColumnLoader(vectorCache,
        fileProjection.metadataColumns());
  }

  public ProjectionSet mapMetadataColumns(ProjectionLifecycle projectionDefn) {
    if (metadataColumnLoader == null) {
      return null;
    }
    VectorContainer metadataContainer = metadataColumnLoader.output();
    ProjectionSet projSet = new ProjectionSet(metadataContainer);
    int metadataMap[] = projectionDefn.tableProjection().metadataProjection();
    for (int i = 0; i < metadataContainer.getNumberOfColumns(); i++) {
      projSet.addDirectProjection(i, metadataMap[i]);
    }
    return projSet;
  }

  public void load(int rowCount) {
    if (metadataColumnLoader != null) {
      metadataColumnLoader.load(rowCount);
    }
  }

  public void close() {
    if (metadataColumnLoader != null) {
      metadataColumnLoader.close();
      metadataColumnLoader = null;
    }
  }

}
