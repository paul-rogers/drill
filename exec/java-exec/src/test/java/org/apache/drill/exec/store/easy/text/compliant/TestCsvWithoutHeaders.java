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

  private static final String TEST_FILE_NAME = "case2.csv";

  private static String sampleData[] = {
      "10,foo,bar",
      "20,fred,wilma"
  };

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher).maxParallelization(1));

    // Set up CSV storage plugin using headers.

    TextFormatConfig csvFormat = new TextFormatConfig();
    csvFormat.fieldDelimiter = ',';
    csvFormat.skipFirstLine = false;
    csvFormat.extractHeader = false;

    File testDir = cluster.makeDataDir("data", "csv", csvFormat);
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

}
