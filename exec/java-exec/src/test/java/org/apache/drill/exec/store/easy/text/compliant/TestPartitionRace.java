package org.apache.drill.exec.store.easy.text.compliant;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.rowSet.DirectRowSet;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetReader;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Demonstrates a race condition inherent in the way that partition
 * columns are currently implemented. Two files: one at the root directory,
 * one down one level. Parallelization is forced to two. (Most tests use
 * small files and both files end up being read in the same scanner, which
 * masks the problem shown here.)
 * <p>
 * Depending on which file is read first, the output row may start with
 * or without the partition column. Once the column occurs, it will
 * persist.
 * <p>
 * The solution will be to figure out the max partition depth in the
 * EasySubScan rather than in each scan operator.
 */

public class TestPartitionRace extends BaseCsvTest {

  @BeforeClass
  public static void setup() throws Exception {
    BaseCsvTest.setup(false,  true, 2);

    // Two-level partitioned table

    File rootDir = new File(testDir, PART_DIR);
    rootDir.mkdir();
    buildFile(new File(rootDir, "first.csv"), validHeaders);
    File nestedDir = new File(rootDir, NESTED_DIR);
    nestedDir.mkdir();
    buildFile(new File(nestedDir, "second.csv"), secondFile);
  }

  /**
   * Oddly, when run in a single fragment, the files occur in a
   * stable order, the partition always appars, and it appears in
   * the first column position.
   */
  @Test
  public void testNoRaceV2() throws IOException {
    String sql = "SELECT * FROM `dfs.data`.`%s`";

    try {
      enableV3(false);

      // Loop to run the query 10 times, or until we see the race

      boolean sawMissingPartition = false;
      boolean sawPartitionFirst = false;
      boolean sawPartitionLast = false;

      // Read the two batches.

      Iterator<DirectRowSet> iter = client.queryBuilder().sql(sql, PART_DIR).rowSetIterator();
      for (int j = 0; j < 2; j++) {
        assertTrue(iter.hasNext());
        RowSet rowSet = iter.next();

        // Check location of partition column

        int posn = rowSet.schema().index("dir0");
        if (posn == -1) {
          sawMissingPartition = true;
        } else if (posn == 0) {
          sawPartitionFirst = true;
        } else {
          sawPartitionLast = true;
        }
        rowSet.clear();
      }
      assertFalse(iter.hasNext());

      // When run in a single fragment, the partition column appears
      // all the time, and is in the first column position.

      assertFalse(sawMissingPartition);
      assertTrue(sawPartitionFirst);
      assertFalse(sawPartitionLast);
    } finally {
      client.resetSession(ExecConstants.MIN_READER_WIDTH_KEY);
    }
  }

  /**
   * When forced to run in two fragments, the fun really starts. The
   * partition column (usually) appears in the last column position instead
   * of the first. The partition may or may not occur in the first row
   * depending on which file is read first. The result is that the
   * other columns will jump around. If we tried to create an expected
   * result set, we'd be frustrated because the schema randomly changes.
   * <p>
   * Just to be clear: this behavior is a bug, not a feature. But, it is
   * an established baseline for the "V2" reader.
   */
  @Test
  public void testRaceV2() throws IOException {
    String sql = "SELECT * FROM `dfs.data`.`%s`";

    try {
      enableV3(false);

      // Special test-only feature to force even small scans
      // to use more than one thread. Requires that the max
      // parallelization option be set when starting the cluster.

      client.alterSession(ExecConstants.MIN_READER_WIDTH_KEY, 2);

      // Loop to run the query 10 times, or until we see the race

      boolean sawRootFirst = false;
      boolean sawNestedFirst = false;
      boolean sawMissingPartition = false;
      boolean sawPartitionFirst = false;
      boolean sawPartitionLast = false;
      for (int i = 0; i < 10; i++) {

        // Read the two batches.

        Iterator<DirectRowSet> iter = client.queryBuilder().sql(sql, PART_DIR).rowSetIterator();
        for (int j = 0; j < 2; j++) {
          assertTrue(iter.hasNext());
          RowSet rowSet = iter.next();

          // Check location of partition column

          int posn = rowSet.schema().index("dir0");
          if (posn == -1) {
            sawMissingPartition = true;
          } else if (posn == 0) {
            sawPartitionFirst = true;
          } else {
            sawPartitionLast = true;
          }

          // Figure out which record this is and test accordingly.

          RowSetReader reader = rowSet.reader();
          assertTrue(reader.next());
          String col1 = reader.scalar("a").getString();
          if (col1.equals("10")) {
            if (i == 0) {
              sawRootFirst = true;
            }
          } else {
            if (i == 0) {
              sawNestedFirst = true;
            }
          }
          rowSet.clear();
        }
        assertFalse(iter.hasNext());
        if (sawMissingPartition &&
            sawPartitionFirst &&
            sawPartitionLast &&
            sawRootFirst &&
            sawNestedFirst) {
          // The following should appear most of the time.
          System.out.println("All variations occurred");
          return;
        }
      }

      // If you see this, maybe something got fixed. Or, maybe the
      // min parallelization hack above stopped working.
      // Or, you were just unlucky and can try the test again.
      // We print messages, rather than using assertTrue, to avoid
      // introducing a flaky test.

      System.out.println("Some variations did not occur");
      System.out.println(String.format("Missing partition: %s", sawMissingPartition));
      System.out.println(String.format("Partition first: %s", sawPartitionFirst));
      System.out.println(String.format("Partition last: %s", sawPartitionLast));
      System.out.println(String.format("Outer first: %s", sawRootFirst));
      System.out.println(String.format("Nested first: %s", sawNestedFirst));
    } finally {
      client.resetSession(ExecConstants.MIN_READER_WIDTH_KEY);
    }
  }

  @Test
  @Ignore("not yet")
  public void testRaceV3() throws IOException {
    String sql = "SELECT * FROM `dfs.data`.`%s`";

    TupleMetadata rootSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .buildSchema();
    TupleMetadata nestedSchema = new SchemaBuilder()
        .addNullable("dir0", MinorType.VARCHAR)
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .buildSchema();

    try {
      enableV3(false);
      client.alterSession(ExecConstants.MIN_READER_WIDTH_KEY, 2);

      // Loop to run the query 10 times, or until we see the race

      boolean sawRootFirst = false;
      boolean sawNestedFirst = false;
      boolean sawPartitionFirst = false;
      boolean sawPartitionLast = false;
      for (int i = 0; i < 10; i++) {
        TupleMetadata expectedSchema = rootSchema;

        // Read the two batches.

        Iterator<DirectRowSet> iter = client.queryBuilder().sql(sql, PART_DIR).rowSetIterator();
        for (int j = 0; j < 2; j++) {
          assertTrue(iter.hasNext());
          RowSet rowSet = iter.next();

          // Figure out which record this is and test accordingly.

          RowSetReader reader = rowSet.reader();
          assertTrue(reader.next());
          String col1 = reader.scalar("a").getString();
          if (col1.equals("10")) {
//            sawRace = firstFile == 2;
//            firstFile = 1;
            RowSetBuilder builder = new RowSetBuilder(client.allocator(), expectedSchema);
            if (expectedSchema == rootSchema) {
              builder.addRow("10", "foo", "bar");
            } else {
              builder.addRow(null, "10", "foo", "bar");
            }
            RowSetUtilities.verify(builder.build(), rowSet);
          } else {
//            sawRace = firstFile == 1;
//            firstFile = 2;
            expectedSchema = nestedSchema;
            RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
                .addRow(NESTED_DIR, "20", "fred", "wilma")
                .build();
            RowSetUtilities.verify(expected, rowSet);
          }
        }
        assertFalse(iter.hasNext());
      }
//      if (!sawRace) {
//        // If didn't see the race, just print a warning to avoid this test becoming flaky.
//        System.out.println("Didn't see the parition race condition? Fixed? Min width regression?");
//      }
    } finally {
      client.resetSession(ExecConstants.MIN_READER_WIDTH_KEY);
    }
  }
}
