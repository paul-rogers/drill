/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.rpc.user;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileFilter;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TemporaryTablesAutomaticDropTest extends BaseTestQuery {

  @Before
  public void init() throws Exception {
    Properties overrideProps = new Properties();
    overrideProps.setProperty(ExecConstants.DEFAULT_TEMPORARY_WORKSPACE, TEMP_SCHEMA);
    updateTestCluster(1, DrillConfig.create(overrideProps));
  }

  @Test
  public void testAutomaticDropWhenClientIsClosed() throws Exception {
    final File temporaryTableLocation = createAndCheckTemporaryTable("client_closed");
    closeClient();
    assertFalse("Temporary table folder should be absent", temporaryTableLocation.exists());
  }

  @Test
  public void testAutomaticDropWhenDrillbitIsClosed() throws Exception {
    final File temporaryTableLocation = createAndCheckTemporaryTable("drillbit_closed");
    bits[0].close();
    assertFalse("Temporary table folder should be absent", temporaryTableLocation.exists());
  }

  private File createAndCheckTemporaryTable(String suffix) throws Exception {
    final String temporaryTableName = "temporary_table_automatic_drop_" + suffix;
    test("create TEMPORARY table %s.%s as select 'A' as c1 from (values(1))", TEMP_SCHEMA, temporaryTableName);
    File workspaceLocation = new File(getDfsTestTmpSchemaLocation());
    File[] files = workspaceLocation.listFiles(new FileFilter() {
      @Override
      public boolean accept(File path) {
        return path.isDirectory() && path.getName().startsWith(temporaryTableName);
      }
    });
    assertNotNull("Should have find directories matching temporary table name " + temporaryTableName, files);
    assertEquals("Only one directory should match temporary table name " + temporaryTableName, 1, files.length);
    assertTrue("Temporary table folder should exist", files[0].exists());
    return files[0];
  }

}
