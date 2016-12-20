package org.apache.drill;

import java.io.IOException;

import org.apache.drill.ClusterFixture.FixtureBuilder;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.test.DrillTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Base class for tests that use a single cluster fixture for a set of
 * tests. Extend your test case directly from {@link DrillTest} if you
 * need to start up and shut down a cluster multiple times.
 * <p>
 * To create a test with a single cluster config, do the following:
 * <pre><code>
 * public class YourTest extends ClusterTest {
 *   {@literal @}BeforeClass
 *   public static setup( ) throws Exception {
 *     FixtureBuilder builder = ClusterFixture.builder()
 *       // Set options, etc.
 *       ;
 *     startCluster(builder);
 *   }
 *
 *   // Your tests
 * }
 * </code></pre>
 * This class takes care of shutting down the cluster at the end of the test.
 * <p>
 * The simplest possible setup:
 * <pre><code>
 *   {@literal @}BeforeClass
 *   public static setup( ) throws Exception {
 *     startCluster(ClusterFixture.builder( ));
 *   }
 * </code></pre>
 * <p>
 * If you need to start the cluster with different (boot time) configurations,
 * do the following instead:
 * <pre><code>
 * public class YourTest extends DrillTest {
 *   {@literal @}Test
 *   public someTest() throws Exception {
 *     FixtureBuilder builder = ClusterFixture.builder()
 *       // Set options, etc.
 *       ;
 *     try(ClusterFixture cluster = builder.build) {
 *       // Tests here
 *     }
 *   }
 * }
 * </code></pre>
 * The try-with-resources block ensures that the cluster is shut down at
 * the end of each test method.
 */

public class ClusterTest extends DrillTest {

  protected static ClusterFixture cluster;

  protected static void startCluster(FixtureBuilder builder) throws Exception {
    cluster = builder.build();
  }

  @AfterClass
  public static void shutdown() throws Exception {
    AutoCloseables.close(cluster);
  }

  /**
   * Convenience method when converting classic tests to use the
   * cluster fixture.
   * @return a test builder that works against the cluster fixture
   */

  public TestBuilder testBuilder() {
    return cluster.testBuilder();
  }

  /**
   * Convenience method when converting classic tests to use the
   * cluster fixture.
   * @return the contents of the resource text file
   */

  public String getFile(String resource) throws IOException {
    return ClusterFixture.getResource(resource);
  }

  public void test(String sqlQuery) throws Exception {
    cluster.runQueries(sqlQuery);
  }

  public static void test(String query, Object... args) throws Exception {
    cluster.queryBuilder().sql(query, args).run( );
  }

}
