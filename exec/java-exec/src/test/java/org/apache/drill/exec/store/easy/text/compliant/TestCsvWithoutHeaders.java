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
package org.apache.drill.exec.store.easy.text.compliant;

import java.io.File;
import java.io.IOException;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.easy.text.TextFormatPlugin.TextFormatConfig;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetUtilities;

import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

//CSV reader now hosted on the row set framework
@Category(RowSetTests.class)
public class TestCsvWithoutHeaders extends ClusterTest {

  private static final String TEST_FILE_NAME = "simple.csv";

  private static String sampleData[] = {
      "10,foo,bar",
      "20,fred,wilma"
  };

  private static String raggedRows[] = {
      "10,dino",
      "20,foo,bar",
      "30"
  };

  private static File testDir;

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher).maxParallelization(1));

    // Set up CSV storage plugin without headers.

    TextFormatConfig csvFormat = new TextFormatConfig();
    csvFormat.fieldDelimiter = ',';
    csvFormat.skipFirstLine = false;
    csvFormat.extractHeader = false;

    testDir = cluster.makeDataDir("data", "csv", csvFormat);
    TestCsvWithHeaders.buildFile(new File(testDir, TEST_FILE_NAME), sampleData);
  }

  /**
   * Verify that the wildcard expands to the `columns` array
   */
  @Test
  public void testWildcard() throws IOException {
    String sql = "SELECT * FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("columns", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addSingleCol(strArray("10", "foo", "bar"))
        .addSingleCol(strArray("20", "fred", "wilma"))
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testColumns() throws IOException {
    String sql = "SELECT columns FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("columns", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addSingleCol(strArray("10", "foo", "bar"))
        .addSingleCol(strArray("20", "fred", "wilma"))
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testWildcardAndMetadata() throws IOException {
    String sql = "SELECT *, filename FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("columns", MinorType.VARCHAR)
        .add("filename", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(strArray("10", "foo", "bar"), TEST_FILE_NAME)
        .addRow(strArray("20", "fred", "wilma"), TEST_FILE_NAME)
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testColumnsAndMetadata() throws IOException {
    String sql = "SELECT columns, filename FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("columns", MinorType.VARCHAR)
        .add("filename", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(strArray("10", "foo", "bar"), TEST_FILE_NAME)
        .addRow(strArray("20", "fred", "wilma"), TEST_FILE_NAME)
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testSpecificColumns() throws IOException {
    String sql = "SELECT columns[0], columns[2] FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("EXPR$0", MinorType.VARCHAR)
        .addNullable("EXPR$1", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("10", "bar")
        .addRow("20", "wilma")
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testRaggedRows() throws IOException {
    String fileName = "ragged.csv";
    TestCsvWithHeaders.buildFile(new File(testDir, fileName), raggedRows);
    String sql = "SELECT columns FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, fileName).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("columns", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addSingleCol(strArray("10", "dino"))
        .addSingleCol(strArray("20", "foo", "bar"))
        .addSingleCol(strArray("30"))
        .build();
    RowSetUtilities.verify(expected, actual);
  }
}
