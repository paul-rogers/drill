package org.apache.drill.exec.store.mock;

import static org.junit.Assert.*;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.rowSet.DirectRowSet;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

public class TestMockDataSource extends ClusterTest {

//  @Test
//  public void testLegacy() throws Exception {
//    try (ClusterFixture cluster = ClusterFixture.standardCluster();
//         ClientFixture client = cluster.clientFixture()) {
//      String sql = "SELECT id_i, name_s10 FROM `mock`.`employees_10K` ORDER BY id_i";
//
//      QuerySummary summary = client.queryBuilder().sql(sql).run();
//      assertEquals(10_000, summary.recordCount());
//    }
//  }

  /**
   * Verify the SQL version of the mock data source. Can't check the actual
   * data (it is random), but we can check row count and schema.
   *
   * @throws Exception if anything goes wrong
   */
  @Test
  public void testExtendedSql() throws Exception {
    try (ClusterFixture cluster = ClusterFixture.standardCluster();
         ClientFixture client = cluster.clientFixture()) {
      String sql = "SELECT id_i, name_s10 FROM `mock`.`employees_10K`";

//      System.out.println(client.queryBuilder().sql(sql).explainJson());
      DirectRowSet results = client.queryBuilder().sql(sql).rowSet();
      assertEquals(10_000, results.rowCount());

      BatchSchema expected = new SchemaBuilder()
          .add("id_i", MinorType.INT)
          .add("name_s10", MinorType.VARCHAR, 10)
          .build();

      // Note: assertEquals does not work here. See DRILL-5525

      assertTrue(results.batchSchema().isEquivalent(expected));
      results.clear();
    }
  }

}
