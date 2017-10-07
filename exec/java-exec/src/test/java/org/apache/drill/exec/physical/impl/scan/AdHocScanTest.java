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
package org.apache.drill.exec.physical.impl.scan;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;

import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.apache.drill.test.QueryResultSet;
import org.apache.drill.test.rowSet.RowSet;
import org.junit.BeforeClass;
import org.junit.Test;

public class AdHocScanTest extends ClusterTest {

  @BeforeClass
  public static void setupTestFiles() {
    dirTestWatcher.copyResourceToRoot(Paths.get("store", "json"));
    dirTestWatcher.copyResourceToRoot(Paths.get("vector","complex", "writer"));
  }

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
    cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "csv");
    cluster.defineWorkspace("dfs", "res", "/Users/paulrogers/git/drill/exec/java-exec/src/test/resources", "json");
    cluster.defineWorkspace("dfs", "tf", "/Users/paulrogers/git/drill-test-framework/framework/resources/Datasources", "psv");
    cluster.defineWorkspace("dfs", "drillTestDirP1", "/Users/paulrogers/work/data/p1tests", "csv");
  }

  @Test
  public void test_sth() throws Exception {
    String sql = "select * from `dfs`.`data`.`/lineitem_hierarchical_intstring` where (dir0=1993 and columns[0]>29600) or (dir0=1994 and columns[0]>29700)";
    QueryResultSet results = client.queryBuilder().sql(sql).resultSet();
    for (;;) {
      RowSet rowSet = results.next();
      if (rowSet == null) {
        break;
      }
      rowSet.print();
      rowSet.clear();
    }
    System.out.println(results.recordCount());
    results.close();
  }

  @Test
  public void test_wide_columns_general_q2() throws Exception {
    String sql = "select count(*) from `dfs`.`data`.`100000.tbl`";
    QueryResultSet results = client.queryBuilder().sql(sql).resultSet();
    for (;;) {
      RowSet rowSet = results.next();
      if (rowSet == null) {
        break;
      }
      rowSet.print();
      rowSet.clear();
    }
    System.out.println(results.recordCount());
    results.close();
  }

  // Functional/table_function/positive/drill-3149_10.q
  // select * from table(`table_function/colons.txt`(type=>'text',lineDelimiter=>'\\'))

  @Test
  public void test_DRILL_3149_10() throws Exception {
    String sql = "select * from table(`dfs`.`data`.`colons.txt`(type=>'text',lineDelimiter=>'\\'))";
    QueryResultSet results = client.queryBuilder().sql(sql).resultSet();
    for (;;) {
      RowSet rowSet = results.next();
      if (rowSet == null) {
        break;
      }
      rowSet.print();
      rowSet.clear();
    }
    System.out.println(results.recordCount());
    results.close();
  }


  @Test
  public void test_window_lead() throws Exception {
//    String sql = "select line_no from `dfs.res`.`/window/b4.p4` order by sub, employee_id";
//    String sql = "select lead(line_no) over(order by sub, employee_id) as `lead` from `dfs.res`.`/window/b4.p4`";
    String sql = "select * from `dfs.res`.`window/b4.p4.lag.pby.oby.tsv`";
    runAndPrint(sql);
  }

  @Test
  public void test_cast() throws Exception {
//    String sql = "select line_no from `dfs.res`.`/window/b4.p4` order by sub, employee_id";
//    String sql = "select lead(line_no) over(order by sub, employee_id) as `lead` from `dfs.res`.`/window/b4.p4`";
//    String sql = "select CAST(`columns`[0] AS INTEGER) AS `student_id`, CAST(`columns`[1] AS VARCHAR(30)) AS `name`, CAST(`columns`[2] AS INTEGER) AS `age`, CAST(`columns`[3] AS DOUBLE) AS `gpa`, CAST(`columns`[4] AS BIGINT) AS `studentnum`, CAST(`columns`[5] AS TIMESTAMP) AS `create_time` from `dfs.tf`.`limit0/p1tests/student.csv`";
//    String sql = "SELECT * FROM `dfs.drillTestDirP1`.`student_csv_v`";
    String sql = "select trunc(student_id,1),trunc(age,1), trunc(gpa,1),trunc(studentnum,5) from `dfs.drillTestDirP1`.`student_csv_v` where student_id=10";
    runAndPrint(sql);
  }

  @Test
  public void test_wide_columns_general_q1() {
    String sql = "select * from `dfs`.`data`.`100000.tbl`";
    runAndPrint(sql);
  }

  @Test
  public void test_partition_pruning_textSelectStartFromPartition() {
    String sql = "select * from `dfs`.`data`.`lineitem_hierarchical_intstring` where (dir0=1993 and columns[0]>29600) or (dir0=1994 and columns[0]>29700)";
    runAndPrint(sql);
  }

  @Test
  public void test_3wayjoin_DRILL_1421() {
    String sql1 = "select * from `dfs.tf`.`/text_storage/DRILL-1421.tbl`";
    runSummary(sql1);
    sql1 = "select * from `dfs.tf`.`/text_storage/rankings.tbl`";
    runSummary(sql1);
    sql1 = "select * from `dfs.tf`.`/text_storage/uservisits.tbl`";
    runSummary(sql1);
    String sql = "select r.columns[1] from `dfs.tf`.`/text_storage/rankings.tbl` r, `dfs.tf`.`/text_storage/uservisits.tbl` u, `dfs.tf`.`/text_storage/DRILL-1421.tbl` t where r.columns[1]=u.columns[1] and r.columns[0] = t.columns[1]";
//    String sql = "select r.columns[1] from `dfs.tf`.`/text_storage/rankings.tbl` r, `dfs.tf`.`/text_storage/DRILL-1421.tbl` t where r.columns[0] = t.columns[1] and r.columns[0] = '12'";
    runAndPrint(sql);
  }

  @Test
  public void simpleJsonTest() {
    String sql = "select * from `cp`.`jsoninput/input1.json`";
    runAndPrint(sql);
  }

  @Test
  public void flattenTest() throws IOException {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(dirTestWatcher.getRootDir(), "empty_array_all_text_mode.json")))) {
      writer.write("{ \"a\": { \"b\": { \"c\": [] }, \"c\": [] } }");
    }
    String query = "select t.a.b.c as c from dfs.`empty_array_all_text_mode.json` t";
    client.alterSession("store.json.all_text_mode", true);
    runAndPrint(query);
  }

  @Test
  public void testAllTextMode() throws IOException {
    String query = "select * from cp.`store/json/schema_change_int_to_string.json`";
    client.alterSession("store.json.all_text_mode", true);
    runAndPrint(query);
  }

  @Test
  public void readComplexWithStar() throws IOException {
    String query = "select * from cp.`store/json/test_complex_read_with_star.json`";
    runAndPrint(query);
  }

  @Test
  public void testFieldSelectionBug() throws Exception {
    String sql = "select t.field_4.inner_3 as col_1, t.field_4 as col_2 from cp.`store/json/schema_change_int_to_string.json` t";
    System.out.println(client.queryBuilder().sql(sql).explainJson());
    runAndPrint(sql);
  }

  @Test
  public void schemaChangeValidate() throws Exception {
    String sql = "select b from dfs.`vector/complex/writer/schemaChange/";
    runAndPrint(sql);
  }

  private void runSummary(String sql) {
    try {
      System.out.println(sql);
      QuerySummary summary = client.queryBuilder().sql(sql).run();
      System.out.println("Rows: " + summary.recordCount());
    } catch (Exception e1) {
      throw new IllegalStateException(e1);
    }
  }
}
