package org.apache.drill.exec.store.sumo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetReader;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TestSumoPlugin extends ClusterTest {
  static final List<String> CREDS = loadSumoCreds();

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher);
    startCluster(builder);
    StoragePluginRegistry pluginRegistry = cluster.drillbit().getContext().getStorage();

    SumoStoragePluginConfig config =
        new SumoStoragePluginConfig(CREDS.get(0), CREDS.get(1), CREDS.get(2),
            "America/Los_Angeles", "-6m", "30s", false, 0);
    config.setEnabled(true);
    pluginRegistry.createOrUpdate(SumoStoragePluginConfig.NAME, config, true);
  }

  /**
   * For security, credentials must be in a separate file in your home folder.
   * Format: three lines:<code><pre>
   * &lt;endpoint>
   * &lt;access id>
   * &lt;access key>
   * </pre></code>
   */
  private static List<String> loadSumoCreds(){
    File file = new File(System.getProperty("user.home"), "sumo-creds.txt");
    assertTrue(file.exists());
    List<String> creds = new ArrayList<>();
    try (BufferedReader in = new BufferedReader(new FileReader(file));) {
      for (int i = 0; i < 3; i++) {
        String line = in.readLine();
        assertNotNull(line);
        creds.add(line);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
    return creds;
  }

  @Test
  public void test() throws Exception {
    long start = System.currentTimeMillis();
    String sql = "SELECT * FROM sumo.logQuery1 LIMIT 10";
    RowSet result = queryBuilder().sql(sql).rowSet();
    System.out.println(String.format("Total ms.: %d", System.currentTimeMillis() - start));
    result.print();
    result.clear();
  }

  @SuppressWarnings("unused")
  private void extendedPrint(RowSet result) {
    RowSetReader reader = result.reader();
    while (reader.next()) {
      TupleMetadata schema = result.schema();
      for (int i = 0; i < schema.size(); i++) {
        System.out.println(String.format("%s (%s): %s",
            schema.metadata(i).name(),
            schema.metadata(i).majorType().getMinorType().name(),
            reader.column(i).getAsString()
            ));
      }
      System.out.println();
    }
  }

  @Test
  public void testViewProject() throws Exception {
    String sql = "SELECT `_collectorid` AS collector, `_messagetime` AS ts, " +
                 "`_raw` AS message, _messagecount as mc\n" +
                 "FROM sumo.logQuery1\n" +
                 "LIMIT 10";
    RowSet result = queryBuilder().sql(sql).rowSet();
    result.print();
    assertFalse(result.rowCount() == 0);
    TupleMetadata schema = result.schema();
    assertEquals(4, schema.size());

    ColumnMetadata col = schema.metadata(0);
    assertEquals("collector", col.name());
    assertEquals(MinorType.BIGINT, col.majorType().getMinorType());

    ColumnMetadata ts = schema.metadata(1);
    assertEquals("ts", ts.name());
    assertEquals(MinorType.TIMESTAMP, ts.majorType().getMinorType());

    ColumnMetadata msg = schema.metadata(2);
    assertEquals("message", msg.name());
    assertEquals(MinorType.VARCHAR, msg.majorType().getMinorType());

    ColumnMetadata mc = schema.metadata(3);
    assertEquals("mc", mc.name());
    assertEquals(MinorType.INT, mc.majorType().getMinorType());

    result.clear();
  }

  @Test
  public void testAgg() throws Exception {
    String sql = "SELECT * FROM sumo.aggQuery1 LIMIT 10";
    RowSet result = queryBuilder().sql(sql).rowSet();
    result.print();

    result.clear();
  }

  @Test
  public void testAggWithAlias() throws Exception {
    String sql = "SELECT _sourceHost AS host, _count as cnt FROM sumo.aggQuery1 LIMIT 10";
    RowSet result = queryBuilder().sql(sql).rowSet();
    result.print();

    result.clear();
  }

  @Test
  public void testSumoQueryJson() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    SumoQuery query = SumoSchemaFactory.dummySchema.get("aggQuery1");
    String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(query);
    System.out.println(json);
  }

  @Test
  public void testSumoConfig() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    SumoStoragePluginConfig config = new SumoStoragePluginConfig(CREDS.get(0), "<my id>", "<my key>",
        "America/Los_Angeles", "-6m", "30s", false, 15);
    String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(config);
    //System.out.println(json);
    SumoStoragePluginConfig deser = mapper.reader().forType(SumoStoragePluginConfig.class).readValue(json);
    assertEquals(config, deser);
  }

  @Test
  public void testAggWithRelTimes() throws Exception {
    String sql = "SELECT _sourceHost AS host, _count as cnt\n" +
                  "FROM sumo.aggQuery1\n" +
                  "WHERE startTime = '2019-11-24T13:10:00'\n" +
                  "  AND endTime = '20s'\n" +
                  "LIMIT 10";
    System.out.println(sql);
    RowSet result = queryBuilder().sql(sql).rowSet();
    result.print();

    result.clear();
  }

  @Test
  public void testAggQueryWithRelTimes() throws Exception {
    String sql = "SELECT _sourceHost AS host, _count as cnt\n" +
                  "FROM sumo.logs\n" +
                  "WHERE query = '* | count _sourceHost'\n" +
                  "  AND startTime = '2019-11-24T13:10:00'\n" +
                  "  AND endTime = '20s'\n" +
                  "LIMIT 3";
    System.out.println(sql);
    RowSet result = queryBuilder().sql(sql).rowSet();
    result.print();

    result.clear();
  }

  @Test
  public void testAggQueryWithDefaultTimes() throws Exception {
    String sql = "SELECT _sourceHost AS host, _count as cnt\n" +
                  "FROM sumo.logs\n" +
                  "WHERE query = '* | count _sourceHost'\n" +
                  "LIMIT 3";
    System.out.println(sql);
    RowSet result = queryBuilder().sql(sql).rowSet();
    result.print();

    result.clear();
  }

  @Test
  public void testViewAbsRelTimes() throws Exception {
    String sql = "SELECT `_collectorid` AS collector, `_messagetime` AS ts, " +
                 "`_raw` AS message, _messagecount as mc\n" +
                 "FROM sumo.logQuery1\n" +
                 "WHERE startTime = '2019-11-24T13:10:00'\n" +
                 "  AND endTime = '20s'\n" +
                 "LIMIT 10";
    RowSet result = queryBuilder().sql(sql).rowSet();
    result.print();

    result.clear();
  }

  @Test
  public void testViewFilter() throws Exception {
    String sql = "SELECT `_collectorid` AS collector, `_messagetime` AS ts, " +
                 "`_raw` AS message, _messagecount as mc\n" +
                 "FROM sumo.logQuery1\n" +
                 "WHERE _messagecount > 10\n" +
                 "LIMIT 10";
    RowSet result = queryBuilder().sql(sql).rowSet();
    result.print();
    result.clear();
  }
}
