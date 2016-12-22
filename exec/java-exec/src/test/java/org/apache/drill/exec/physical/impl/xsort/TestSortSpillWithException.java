/**
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.drill.ClusterFixture;
import org.apache.drill.ClusterFixture.FixtureBuilder;
import org.apache.drill.ClusterTest;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType;
import org.apache.drill.exec.testing.Controls;
import org.apache.drill.exec.testing.ControlsInjectionUtil;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Testing External Sort's spilling to disk.
 * <br>
 * This class changes the following Drill property to force external sort to spill after the 2nd batch:
 * {@link ExecConstants#EXTERNAL_SORT_SPILL_THRESHOLD} = 1
 * <br>
 * {@link ExecConstants#EXTERNAL_SORT_SPILL_GROUP_SIZE} = 1
 */
public class TestSortSpillWithException extends ClusterTest {
  private static final String TEST_RES_PATH = TestTools.getWorkingPath() + "/src/test/resources";

  @BeforeClass
  public static void setup( ) throws Exception {
    FixtureBuilder builder = ClusterFixture.builder( )
        .configProperty(ExecConstants.EXTERNAL_SORT_SPILL_THRESHOLD, 1) // Unmanaged
        .configProperty(ExecConstants.EXTERNAL_SORT_SPILL_GROUP_SIZE, 1) // Unmanaged
        .configProperty(ExecConstants.EXTERNAL_SORT_BATCH_LIMIT, 1) // Managed
        ;
    startCluster(builder);
  }

  @Test
  public void testSpillLeakLegacy() throws Exception {
    cluster.alterSession(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED_OPTION.getOptionName(), true);
    // inject exception in sort while spilling
    final String controls = Controls.newBuilder()
      .addExceptionOnBit(
          org.apache.drill.exec.physical.impl.xsort.ExternalSortBatch.class,
          org.apache.drill.exec.physical.impl.xsort.ExternalSortBatch.INTERRUPTION_WHILE_SPILLING,
          IOException.class,
          cluster.drillbit( ).getContext().getEndpoint())
      .build();
    ControlsInjectionUtil.setControls(cluster.client( ), controls);
    // run a simple order by query
    try {
      test("select employee_id from dfs_test.`%s/xsort/2batches` order by employee_id", TEST_RES_PATH);
      fail("Query should have failed!");
    } catch (UserRemoteException e) {
      assertEquals(ErrorType.RESOURCE, e.getErrorType());
      assertTrue("Incorrect error message",
        e.getMessage().contains("External Sort encountered an error while spilling to disk"));
    }
  }

  @Test
  public void testSpillLeakManaged() throws Exception {
    cluster.alterSession(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED_OPTION.getOptionName(), false);
    // inject exception in sort while spilling
    final String controls = Controls.newBuilder()
      .addExceptionOnBit(
          org.apache.drill.exec.physical.impl.xsort.managed.ExternalSortBatch.class,
          org.apache.drill.exec.physical.impl.xsort.managed.ExternalSortBatch.INTERRUPTION_WHILE_SPILLING,
          IOException.class,
          cluster.drillbit( ).getContext().getEndpoint())
      .build();
    ControlsInjectionUtil.setControls(cluster.client( ), controls);
    // run a simple order by query
    try {
      test("SELECT id_i, name_s5000 FROM `mock`.`employee_10K` ORDER BY id_i");
//      test("select employee_id from dfs_test.`%s/xsort/2batches` order by employee_id", TEST_RES_PATH);
      fail("Query should have failed!");
    } catch (UserRemoteException e) {
      assertEquals(ErrorType.RESOURCE, e.getErrorType());
      assertTrue("Incorrect error message",
        e.getMessage().contains("External Sort encountered an error while spilling to disk"));
    }
  }
}
