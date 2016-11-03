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
package org.apache.drill.exec.sql;

import com.google.common.collect.Lists;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.store.StorageStrategy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class TestCTTAS extends BaseTestQuery {

  private static FileSystem fs;
  private static FsPermission expectedPermission;

  @BeforeClass
  public static void init() throws Exception {
    Properties overrideProps = new Properties();
    overrideProps.setProperty(ExecConstants.DEFAULT_TEMPORARY_WORKSPACE, TEMP_SCHEMA);
    updateTestCluster(1, DrillConfig.create(overrideProps));

    Configuration conf = new Configuration();
    fs = FileSystem.get(conf);
    expectedPermission = new FsPermission(StorageStrategy.TEMPORARY.getPermission());
  }

  @Test
  public void testSyntax() throws Exception {
    test("create TEMPORARY table temporary_keyword as select 1 from (values(1))");
    test("create TEMPORARY table dfs_test.tmp.temporary_keyword_with_wk as select 1 from (values(1))");
  }

  @Test
  public void testCreateTableWithDifferentStorageFormats() throws Exception {
    List<String> storageFormats = Lists.newArrayList("parquet", "json", "csvh");

    try {
      for (String storageFormat : storageFormats) {
        String tmpTableName = "temp_" + storageFormat;
        test("alter session set `store.format`='%s'", storageFormat);
        test("create TEMPORARY table %s as select 'A' as c1 from (values(1))", tmpTableName);
        checkPermission(tmpTableName);

        testBuilder()
            .sqlQuery("select * from %s", tmpTableName)
            .unOrdered()
            .baselineColumns("c1")
            .baselineValues("A")
            .go();

        testBuilder()
            .sqlQuery("select * from %s", tmpTableName)
            .unOrdered()
            .sqlBaselineQuery("select * from %s.%s", TEMP_SCHEMA, tmpTableName)
            .go();
      }
    } finally {
      test("alter session reset `store.format`");
    }
  }

  @Test(expected = UserRemoteException.class)
  public void testCreateWhenTemporaryTableExistsWithoutSchema() throws Exception {
    String temporaryTableName = "temporary_table_exists_without_schema";
    test("create TEMPORARY table %s as select 'A' as c1 from (values(1))", temporaryTableName);
    try {
      test("create TEMPORARY table %s as select 'A' as c1 from (values(1))", temporaryTableName);
    } catch (UserRemoteException e) {
      assertThat(e.getMessage(), containsString(String.format(
              "VALIDATION ERROR: A table or view with given name [%s]" +
                      " already exists in schema [%s]", temporaryTableName, TEMP_SCHEMA)));
      throw e;
    }
  }

  @Test(expected = UserRemoteException.class)
  public void testCreateWhenTemporaryTableExistsWithSchema() throws Exception {
    String temporaryTableName = "temporary_table_exists_with_schema";
    test("create TEMPORARY table %s.%s as select 'A' as c1 from (values(1))", TEMP_SCHEMA, temporaryTableName);
    try {
      test("create TEMPORARY table %s.%s as select 'A' as c1 from (values(1))", TEMP_SCHEMA, temporaryTableName);
    } catch (UserRemoteException e) {
      assertThat(e.getMessage(), containsString(String.format(
              "VALIDATION ERROR: A table or view with given name [%s]" +
                      " already exists in schema [%s]", temporaryTableName, TEMP_SCHEMA)));
      throw e;
    }
  }

  @Test(expected = UserRemoteException.class)
  public void testCreateWhenPersistentTableExists() throws Exception {
    String persistentTableName = "persistent_table_exists";
    test("create table %s.%s as select 'A' as c1 from (values(1))", TEMP_SCHEMA, persistentTableName);
    try {
      test("create TEMPORARY table %s as select 'A' as c1 from (values(1))", persistentTableName);
    } catch (UserRemoteException e) {
      assertThat(e.getMessage(), containsString(String.format(
          "VALIDATION ERROR: A table or view with given name [%s]" +
              " already exists in schema [%s]", persistentTableName, TEMP_SCHEMA)));
      throw e;
    }
  }

  @Test(expected = UserRemoteException.class)
  public void testCreateWhenViewExists() throws Exception {
    String viewName = "view_exists";
    test("create view %s.%s as select 'A' as c1 from (values(1))", TEMP_SCHEMA, viewName);
    try {
      test("create TEMPORARY table %s as select 'A' as c1 from (values(1))", viewName);
    } catch (UserRemoteException e) {
      assertThat(e.getMessage(), containsString(String.format(
          "VALIDATION ERROR: A table or view with given name [%s]" +
              " already exists in schema [%s]", viewName, TEMP_SCHEMA)));
      throw e;
    }
  }

  @Test(expected = UserRemoteException.class)
  public void testCreatePersistentTableWhenTemporaryTableExists() throws Exception {
    String temporaryTableName = "temporary_table_exists_before_persistent";
    test("create TEMPORARY table %s as select 'A' as c1 from (values(1))", temporaryTableName);
    try {
      test("create table %s.%s as select 'A' as c1 from (values(1))", TEMP_SCHEMA, temporaryTableName);
    } catch (UserRemoteException e) {
      assertThat(e.getMessage(), containsString(String.format(
              "VALIDATION ERROR: A table or view with given name [%s]" +
                      " already exists in schema [%s]", temporaryTableName, TEMP_SCHEMA)));
      throw e;
    }
  }

  @Test(expected = UserRemoteException.class)
  public void testCreateViewWhenTemporaryTableExists() throws Exception {
    String temporaryTableName = "temporary_table_exists_before_view";
    test("create TEMPORARY table %s as select 'A' as c1 from (values(1))", temporaryTableName);
    try {
      test("create view %s.%s as select 'A' as c1 from (values(1))", TEMP_SCHEMA, temporaryTableName);
    } catch (UserRemoteException e) {
      assertThat(e.getMessage(), containsString(String.format(
              "VALIDATION ERROR: A non-view table with given name [%s] already exists in schema [%s]",
              temporaryTableName, TEMP_SCHEMA)));
      throw e;
    }
  }

  @Test
  public void testManualDropWithoutSchema() throws Exception {
    String temporaryTableName = "temporary_table_to_drop_without_schema";
    test("create TEMPORARY table %s as select 'A' as c1 from (values(1))", temporaryTableName);

    testBuilder()
        .sqlQuery("drop table %s", temporaryTableName)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Temporary table [%s] dropped", temporaryTableName))
        .go();
  }

  @Test
  public void testManualDropWithSchema() throws Exception {
    String temporaryTableName = "temporary_table_to_drop_with_schema";
    test("create TEMPORARY table %s.%s as select 'A' as c1 from (values(1))", TEMP_SCHEMA, temporaryTableName);

    testBuilder()
        .sqlQuery("drop table %s.%s", TEMP_SCHEMA, temporaryTableName)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Temporary table [%s] dropped", temporaryTableName))
        .go();
  }

  @Test
  public void testDropTemporaryTableAsViewWithoutException() throws Exception {
    String temporaryTableName = "temporary_table_to_drop_like_view_without_exception";
    test("create TEMPORARY table %s as select 'A' as c1 from (values(1))", temporaryTableName);

    testBuilder()
            .sqlQuery("drop view if exists %s.%s", TEMP_SCHEMA, temporaryTableName)
            .unOrdered()
            .baselineColumns("ok", "summary")
            .baselineValues(false, String.format("View [%s] not found in schema [%s].",
                    temporaryTableName, TEMP_SCHEMA))
            .go();
  }

  @Test(expected = UserRemoteException.class)
  public void testDropTemporaryTableAsViewWithException() throws Exception {
    String temporaryTableName = "temporary_table_to_drop_like_view_with_exception";
    test("create TEMPORARY table %s as select 'A' as c1 from (values(1))", temporaryTableName);

    try {
      test("drop view %s.%s", TEMP_SCHEMA, temporaryTableName);
    } catch (UserRemoteException e) {
      assertThat(e.getMessage(), containsString(String.format(
              "VALIDATION ERROR: [%s] is not a VIEW in schema [%s]", temporaryTableName, TEMP_SCHEMA)));
      throw e;
    }
  }

  private void checkPermission(String tmpTableName) throws IOException {
    File[] files = findTemporaryTableLocation(tmpTableName);
    assertEquals("Only one directory should match temporary table name " + tmpTableName, 1, files.length);
    Path tmpTablePath = new Path(files[0].toURI().getPath());
    FileStatus fileStatus = fs.getFileStatus(tmpTablePath);
    FsPermission permission = fileStatus.getPermission();
    assertEquals("Directory permission should match", expectedPermission, permission);
    RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(tmpTablePath, false);
    while (fileIterator.hasNext()) {
      assertEquals("File permission should match", expectedPermission, fileIterator.next().getPermission());
    }
  }

  private File[] findTemporaryTableLocation(final String tableName) {
    File workspaceLocation = new File(getDfsTestTmpSchemaLocation());
    return workspaceLocation.listFiles(new FileFilter() {
      @Override
      public boolean accept(File path) {
        return path.isDirectory() && path.getName().startsWith(tableName);
      }
    });
  }

}
