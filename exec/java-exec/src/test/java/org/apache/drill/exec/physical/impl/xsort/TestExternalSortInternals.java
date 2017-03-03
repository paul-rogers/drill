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
package org.apache.drill.exec.physical.impl.xsort;

import static org.junit.Assert.*;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.impl.xsort.managed.SortConfig;
import org.apache.drill.test.ConfigBuilder;
import org.apache.drill.test.DrillTest;
import org.junit.Test;

public class TestExternalSortInternals extends DrillTest {

  /**
   * Verify defaults configured in drill-override.conf.
   */
  @Test
  public void testConfigDefaults() {
    DrillConfig drillConfig = DrillConfig.create();
    SortConfig sortConfig = new SortConfig(drillConfig);
    // Zero means no artificial limit
    assertEquals(0, sortConfig.maxMemory());
    // Zero mapped to large number
    assertEquals(Integer.MAX_VALUE, sortConfig.mergeLimit());
    // Default size: 256 MiB
    assertEquals(256 * 1024 * 1024, sortConfig.spillFileSize());
    // Default size: 8 MiB
    assertEquals(8 * 1024 * 1024, sortConfig.spillBatchSize());
    // Default size: 16 MiB
    assertEquals(16 * 1024 * 1024, sortConfig.mergeBatchSize());
  }

  /**
   * Verify that the various constants do, in fact, map to the
   * expected properties, and that the properties are overriden.
   */
  @Test
  public void testConfigOverride() {
    // Verify the various HOCON ways of setting memory
    DrillConfig drillConfig = new ConfigBuilder()
        .put(ExecConstants.EXTERNAL_SORT_MAX_MEMORY, "2000K")
        .put(ExecConstants.EXTERNAL_SORT_MERGE_LIMIT, 10)
        .put(ExecConstants.EXTERNAL_SORT_SPILL_FILE_SIZE, "10M")
        .put(ExecConstants.EXTERNAL_SORT_SPILL_BATCH_SIZE, 500_000)
        .put(ExecConstants.EXTERNAL_SORT_MERGE_BATCH_SIZE, 600_000)
        .build();
    SortConfig sortConfig = new SortConfig(drillConfig);
    assertEquals(2000 * 1024, sortConfig.maxMemory());
    assertEquals(10, sortConfig.mergeLimit());
    assertEquals(10 * 1024 * 1024, sortConfig.spillFileSize());
    assertEquals(500_000, sortConfig.spillBatchSize());
    assertEquals(600_000, sortConfig.mergeBatchSize());
  }

  /**
   * Some properties have hard-coded limits. Verify these limits.
   */
  @Test
  public void testConfigLimits() {
    DrillConfig drillConfig = new ConfigBuilder()
        .put(ExecConstants.EXTERNAL_SORT_MERGE_LIMIT, SortConfig.MIN_MERGE_LIMIT - 1)
        .put(ExecConstants.EXTERNAL_SORT_SPILL_FILE_SIZE, SortConfig.MIN_SPILL_FILE_SIZE - 1)
        .put(ExecConstants.EXTERNAL_SORT_SPILL_BATCH_SIZE, SortConfig.MIN_SPILL_BATCH_SIZE - 1)
        .put(ExecConstants.EXTERNAL_SORT_MERGE_BATCH_SIZE, SortConfig.MIN_MERGE_BATCH_SIZE - 1)
        .build();
    SortConfig sortConfig = new SortConfig(drillConfig);
    assertEquals(SortConfig.MIN_MERGE_LIMIT, sortConfig.mergeLimit());
    assertEquals(SortConfig.MIN_SPILL_FILE_SIZE, sortConfig.spillFileSize());
    assertEquals(SortConfig.MIN_SPILL_BATCH_SIZE, sortConfig.spillBatchSize());
    assertEquals(SortConfig.MIN_MERGE_BATCH_SIZE, sortConfig.mergeBatchSize());
  }

}
