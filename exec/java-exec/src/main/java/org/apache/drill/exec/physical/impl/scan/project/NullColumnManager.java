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

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.physical.impl.scan.project.NullColumnLoader.NullColumnSpec;
import org.apache.drill.exec.physical.impl.scan.project.RowBatchMerger.VectorSource;
import org.apache.drill.exec.physical.rowSet.impl.ResultVectorCacheImpl;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;

/**
 * Manages null columns by creating a null column loader for each
 * set of non-empty null columns. This class acts as a scan-wide
 * facade around the per-schema null column loader.
 */

public class NullColumnManager implements VectorSource {

  /**
   * Creates null columns if needed.
   */

  private NullColumnLoader nullColumnLoader;
  private final ResultVectorCacheImpl vectorCache;
  private VectorContainer outputContainer;

  /**
   * The reader-specified null type if other than the default.
   */

  private final MajorType nullType;

  public NullColumnManager(ResultVectorCacheImpl vectorCache,
      MajorType nullType) {
    this.vectorCache = vectorCache;
    this.nullType = nullType;
  }

  public void define(List<NullColumnSpec> nullCols) {
    close();

    // If no null columns for this schema, no need to create
    // the loader.

    if (! nullCols.isEmpty()) {
      nullColumnLoader = new NullColumnLoader(vectorCache, nullCols, nullType);
    }
  }

  public void load(int rowCount) {
    if (nullColumnLoader != null) {
      outputContainer = nullColumnLoader.load(rowCount);
    }
  }

  public void close() {
    if (nullColumnLoader != null) {
      nullColumnLoader.close();
      nullColumnLoader = null;
    }
    if (outputContainer != null) {
      outputContainer.zeroVectors();
      outputContainer = null;
    }
  }

  @Override
  public VectorContainer container() {
    return outputContainer;
  }
}
