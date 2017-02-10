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
package org.apache.drill.exec.planner;

import static org.junit.Assert.*;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.planner.cost.DrillRelMdSelectivity;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.DrillTest;
import org.apache.drill.test.FixtureBuilder;
import org.apache.drill.test.ProfileParser;
import org.apache.drill.test.ProfileParser.OpDefInfo;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDefaultSelectivity extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
        .configProperty(ExecConstants.QUERY_PROFILE_OPTION, "sync")
        .maxParallelization(1);
    startCluster(builder);
  }

  @Test
  public void testOperators() throws Exception {
    verifyReduction( "id_i = 10", DrillRelMdSelectivity.PROB_A_EQ_B );
    verifyReduction( "id_i <> 10", DrillRelMdSelectivity.PROB_A_NE_B );
    verifyReduction( "id_i < 10", DrillRelMdSelectivity.PROB_A_LT_B );
    verifyReduction( "id_i > 10", DrillRelMdSelectivity.PROB_A_GT_B );
    verifyReduction( "id_i <= 10", DrillRelMdSelectivity.PROB_A_LE_B );
    verifyReduction( "id_i >= 10", DrillRelMdSelectivity.PROB_A_GE_B );
    verifyReduction( "id_i IN (10)", DrillRelMdSelectivity.PROB_A_IN_B );
    verifyReduction( "id_i IN (10, 20)", 2 * DrillRelMdSelectivity.PROB_A_IN_B );
    verifyReduction( "id_i IN (11, 12, 13, 14, 15, 16, 16, 18, 19, 20)", 1.0 );
    verifyReduction( "name_s20 LIKE 'foo'", DrillRelMdSelectivity.PROB_A_LIKE_B );
    verifyReduction( "name_s20 NOT LIKE 'foo'", DrillRelMdSelectivity.PROB_A_NOT_LIKE_B );
    verifyReduction( "name_s20 IS NULL", DrillRelMdSelectivity.PROB_A_IS_NULL );
    verifyReduction( "name_s20 IS NOT NULL", DrillRelMdSelectivity.PROB_A_NOT_NULL );
    verifyReduction( "NOT ( id_i = 10 )", 1.0 - DrillRelMdSelectivity.PROB_A_EQ_B );
    verifyReduction( "flag_b IS TRUE", 1.0 - DrillRelMdSelectivity.PROB_A_IS_TRUE );
    verifyReduction( "flag_b IS FALSE", 1.0 - DrillRelMdSelectivity.PROB_A_IS_FALSE );
    verifyReduction( "flag_b IS NOT TRUE", 1.0 - DrillRelMdSelectivity.PROB_A_NOT_TRUE );
    verifyReduction( "flag_b IS NOT FALSE", 1.0 - DrillRelMdSelectivity.PROB_A_NOT_FALSE );
  }

  @Test
  public void testSample1() throws Exception {
    String where = "col1_s20 in ('Value1','Value2','Value3','Value4','Value5','Value6','Value7','Value8','Value9')\n" + // .15 * 9 = 100%
        "AND col2_i <=3\n" + // 45%
        "AND col3_s1 = 'Y'\n" + // 15%
        "AND col4_s1 = 'Y'\n" + // 15%
        "AND col5_s6 not like '%str1%'\n" + // 85%
        "AND col5_s6 not like '%str2%'\n" + // 85%
        "AND col5_s6 not like '%str3%'\n" + // 85%
        "AND col5_s6 not like '%str4%'\n"; // 85%
    String sql = "SELECT col1_s20, col2_i, col3_s1, col4_s1, col6_s6 FROM `mock`.`example_10K`\n" +
                 "WHERE " + where;
    double expected = 1.0 *
                      DrillRelMdSelectivity.PROB_A_LE_B *
                      Math.pow(DrillRelMdSelectivity.PROB_A_EQ_B, 2) *
                      Math.pow(DrillRelMdSelectivity.PROB_A_NOT_LIKE_B, 4);
    System.out.println("Sample 1 reduction: " + expected);
    verifyReductionQuery(sql, "sample 1", expected);
  }

  @Test
  public void verifyBooleanBehavior() {
    String base = "SELECT * FROM `cp`.`planner/tf.json`";
    QuerySummary summary = queryBuilder().sql(base).run();
    assertEquals( 4, summary.recordCount() );
    summary = queryBuilder().sql(base + " WHERE col IS TRUE").run();
    assertEquals( 1, summary.recordCount() );
    summary = queryBuilder().sql(base + " WHERE col IS NULL").run();
    assertEquals( 2, summary.recordCount() );
    summary = queryBuilder().sql(base + " WHERE col IS FALSE").run();
    assertEquals( 1, summary.recordCount() );
    summary = queryBuilder().sql(base + " WHERE NOT (col IS TRUE)").run();
    assertEquals( 3, summary.recordCount() );
    summary = queryBuilder().sql(base + " WHERE NOT (col IS FALSE)").run();
    assertEquals( 3, summary.recordCount() );
  }

  private void verifyReduction(String where, double expected) throws Exception {
    String sql = "SELECT id_i, name_s20, flag_b FROM `mock`.`customer_10K`";
    if ( where != null ) {
      sql += "WHERE " + where;
    }
    verifyReductionQuery( sql, where, expected );
  }

  private void verifyReductionQuery(String sql, String label, double expected) throws Exception {
//    System.out.println( client.queryBuilder().sql(sql).explainText() );

    // Sad: must run the query to get the estimates.
    ClientFixture cf = client;
    QuerySummary summary = cf.queryBuilder().sql(sql).run();
//    System.out.println(String.format("Results: %,d records, %d batches, %,d ms", summary.recordCount(), summary.batchCount(), summary.runTimeMs() ) );
//    System.out.println("Query ID: " + summary.queryIdString());
    ProfileParser profile = client.parseProfile(summary.queryIdString());
//    profile.printPlan();

    OpDefInfo scan = profile.getOpDefn( "Scan" ).get(0);
    OpDefInfo filter = profile.getOpDefn( "Filter" ).get(0);
    double reduction = filter.estRows / scan.estRows;
    assertEquals( label, reduction, expected, expected / 10 );
  }

  @Test
  public void test2() {
  }

}
