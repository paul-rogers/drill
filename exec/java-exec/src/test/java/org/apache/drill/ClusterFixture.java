/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.drill.BaseTestQuery.SilentListener;
import org.apache.drill.BufferingQueryEventListener.QueryEvent;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.client.PrintingResultsListener;
import org.apache.drill.exec.client.QuerySubmitter.Format;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.AwaitableUserResultsListener;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.StoragePluginRegistryImpl;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.drill.exec.store.mock.MockStorageEngine;
import org.apache.drill.exec.store.mock.MockStorageEngineConfig;
import org.apache.drill.exec.util.TestUtilities;
import org.apache.drill.exec.util.VectorUtil;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Resources;

/**
 * Test fixture to start a Drillbit with provide options, create a client,
 * and execute queries. Can be used in JUnit tests, or in ad-hoc programs.
 * Provides a builder to set the necessary embedded Drillbit and client
 * options, then creates the requested Drillbit and client.
 */

public class ClusterFixture implements AutoCloseable {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ClientFixture.class);
  private static final String ENABLE_FULL_CACHE = "drill.exec.test.use-full-cache";
  private static final int MAX_WIDTH_PER_NODE = 2;

  @SuppressWarnings("serial")
  protected static final Properties TEST_CONFIGURATIONS = new Properties() {
    {

      // Properties here mimic those in drill-root/pom.xml, Surefire plugin
      // configuration. They allow tests to run successfully in Eclipse.

      put(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, "false");
      put(ExecConstants.HTTP_ENABLE, "false");
      put(Drillbit.SYSTEM_OPTIONS_NAME, "org.apache.drill.exec.compile.ClassTransformer.scalar_replacement=on");
      put(QueryTestUtil.TEST_QUERY_PRINTING_SILENT, "true");
      put("drill.catastrophic_to_standard_out", "true");
    }
  };

  public static class RuntimeOption {
    public String key;
    public Object value;

    public RuntimeOption( String key, Object value ) {
      this.key = key;
      this.value = value;
    }
  }

  /**
   * Build a Drillbit and client with the options provided. The simplest
   * builder starts an embedded Drillbit, with the "dfs_test" name space,
   * a max width (parallelization) of 2.
   */

  public static class FixtureBuilder {

    public static Properties defaultProps( ) {
      Properties props = new Properties( );
      props.putAll( TEST_CONFIGURATIONS );
      return props;
    }

    private String configResource;
    private Properties configProps;
    private Properties clientProps;
    private boolean enableFullCache;
    private List<ClusterFixture.RuntimeOption> runtimeSettings;
    private int bitCount = 1;

    /**
     * Use the given configuration properties to start the embedded Drillbit.
     * @param configProps a collection of config properties
     * @return this builder
     * @see {@link #property(String, Object)}
     */

    public FixtureBuilder configProps( Properties configProps ) {
      this.configProps = configProps;
      return this;
    }

    /**
     * Use the given configuration file, stored as a resource, to start the
     * embedded Drillbit. Note that the resource file should have the two
     * following settings to work as a test:
     * <pre><code>
     * drill.exec.sys.store.provider.local.write : false,
     * drill.exec.http.enabled : false
     * </code></pre>
     * It may be more convenient to add your settings to the default
     * config settings with {@link #property(String, Object)}.
     * @param configResource path to the file that contains the
     * config file to be read
     * @return this builder
     * @see {@link #property(String, Object)}
     */

    public FixtureBuilder configResource( String configResource ) {

      // TypeSafe gets unhappy about a leading slash, but other functions
      // require it. Silently discard the leading slash if given to
      // preserve the test writer's sanity.

      this.configResource = trimSlash( configResource );
      return this;
    }

    /**
     * Add an additional boot-time property for the embedded Drillbit.
     * @param key config property name
     * @param value property value
     * @return this builder
     */

    public FixtureBuilder property( String key, Object value ) {
      if ( configProps == null ) {
        configProps = defaultProps( );
      }
      configProps.put(key, value.toString());
      return this;
    }

    /**
     * Specify an optional client property.
     * @param key property name
     * @param value property value
     * @return this builder
     */
    public FixtureBuilder clientProperty( String key, Object value ) {
      if ( clientProps == null ) {
        clientProps = new Properties( );
      }
      clientProps.put(key, value);
      return this;
    }

    /**
     * Provide a runtime configuration option to be set once the Drillbit
     * is started.
     *
     * @param key the name of the runtime option
     * @param value the value of the runtime option
     * @return this builder
     * @see {@link ClusterFixture#alterSession(String, Object)}
     */

    public FixtureBuilder option( String key, Object value ) {
      if ( runtimeSettings == null ) {
        runtimeSettings = new ArrayList<>( );
      }
      runtimeSettings.add( new RuntimeOption( key, value ) );
      return this;
    }

    /**
     * Set the maximum parallelization (max width per node). Defaults
     * to 2.
     *
     * @param n the "max width per node" parallelization option.
     * @return this builder
     */
    public FixtureBuilder maxParallelization(int n) {
      return option( ExecConstants.MAX_WIDTH_PER_NODE_KEY, n );
    }

    public FixtureBuilder enableFullCache( ) {
      enableFullCache = true;
      return this;
    }

    /**
     * The number of Drillbits to start in the cluster.
     *
     * @param n the desired cluster size
     * @return this builder
     */
    public FixtureBuilder clusterSize( int n ) {
      bitCount = n;
      return this;
    }

    /**
     * Create the embedded Drillbit and client, applying the options set
     * in the builder. Best to use this in a try-with-resources block:
     * <pre><code>
     * FixtureBuilder builder = ClientFixture.newBuilder( )
     *   .property( ... );
     * try (ClientFixture client = builder.build()) {
     *   // Do the test
     * }
     * </code></pre>
     *
     * @return
     * @throws Exception
     */
    public ClusterFixture build( ) throws Exception {
      return new ClusterFixture( this );
    }
  }

  public static class QuerySummary {
    private final int records;
    private final int batches;
    private final long ms;

    public QuerySummary(int recordCount, int batchCount, long elapsed) {
      records = recordCount;
      batches = batchCount;
      ms = elapsed;
    }

    public long recordCount( ) { return records; }
    public int batchCount( ) { return batches; }
    public long runTimeMs( ) { return ms; }
  }

  public class QueryBuilder {

    private QueryType queryType;
    private String queryText;

    public QueryBuilder sql(String sql) {
      queryType = QueryType.SQL;
      queryText = sql;
      return this;
    }

    public QueryBuilder physical(String plan) {
      queryType = QueryType.PHYSICAL;
      queryText = plan;
      return this;
    }

    public QueryBuilder sqlResource(String resource) {
      sql(loadResource(resource));
      return this;
    }

    public QueryBuilder physicalResource(String resource) {
      physical(loadResource(resource));
      return this;
    }

    /**
     * Run the query returning just a summary of the results: record count,
     * batch count and run time. Handy when doing performance tests when the
     * validity of the results is verified in some other test.
     *
     * @return the query summary
     */

    public QuerySummary run() {
      return produceSummary(withEventListener());
    }

    /**
     * Run the query and return a list of the result batches. Use
     * if the batch count is small and you want to work with them.
     * @return a list of batches resulting from the query
     * @throws RpcException
     */

    public List<QueryDataBatch> results() throws RpcException {
      Preconditions.checkNotNull(queryType, "Query not provided.");
      Preconditions.checkNotNull(queryText, "Query not provided.");
      return client.runQuery(queryType, queryText);
    }

    /**
     * Run the query with the listener provided. Use when the result
     * count will be large, or you don't need the results.
     *
     * @param listener the Drill listener
     */

    public void withListener(UserResultsListener listener) {
      Preconditions.checkNotNull(queryType, "Query not provided.");
      Preconditions.checkNotNull(queryText, "Query not provided.");
      client.runQuery(queryType, queryText, listener);
    }

    /**
     * Run the query, return an easy-to-use event listener to process
     * the query results. Use when the result set is large. The listener
     * allows the caller to iterate over results in the test thread.
     * (The listener implements a producer-consumer model to hide the
     * details of Drill listeners.)
     *
     * @return the query event listener
     */

    public BufferingQueryEventListener withEventListener( ) {
      BufferingQueryEventListener listener = new BufferingQueryEventListener( );
      withListener(listener);
      return listener;
    }

    public int printCsv() {
      return print(Format.CSV);
    }

    public int print( Format format ) {
      return print(format,20);
    }

    public int print(Format format, int colWidth) {
      return runAndWait( new PrintingResultsListener( config, format, colWidth ) );
    }

    public int runAndWait(UserResultsListener listener) {
      AwaitableUserResultsListener resultListener =
          new AwaitableUserResultsListener(listener);
      withListener( resultListener );
      try {
        return resultListener.await();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }

    /**
     * Submit an "EXPLAIN" statement, and return text form of the
     * plan.
     * @throws Exception if the query fails
     */

    public String explainText() throws Exception {
      return explain(EXPLAIN_PLAN_TEXT);
    }

    /**
     * Submit an "EXPLAIN" statement, and return the JSON form of the
     * plan.
     * @throws Exception if the query fails
     */

    public String explainJson() throws Exception {
      return explain(EXPLAIN_PLAN_JSON);
    }

    public String explain(String format) throws Exception {
      queryText = "EXPLAIN PLAN FOR " + queryText;
      return queryPlan(format);
    }

    private QuerySummary produceSummary(BufferingQueryEventListener listener) {
      long start = System.currentTimeMillis();
      int recordCount = 0;
      int batchCount = 0;
      loop:
      for ( ; ; ) {
        QueryEvent event = listener.get();
        switch ( event.type )
        {
        case BATCH:
          batchCount++;
          recordCount += event.batch.getHeader().getRowCount();
          event.batch.release();
          break;
        case EOF:
          break loop;
        case ERROR:
          event.error.printStackTrace();
          fail( );
          break loop;
        case QUERY_ID:
          break;
        default:
          break;
        }
      }
      long end = System.currentTimeMillis();
      long elapsed = end - start;
      return new QuerySummary( recordCount, batchCount, elapsed );
    }

    /**
     * Submit an "EXPLAIN" statement, and return the column value which
     * contains the plan's string.
     * <p>
     * Cribbed from {@link PlanTestBase#getPlanInString(String, String)}
     * @throws Exception
     */

    protected String queryPlan(String columnName) throws Exception {
      Preconditions.checkArgument(queryType == QueryType.SQL, "Can only explan an SQL query.");
      final List<QueryDataBatch> results = results();
      final RecordBatchLoader loader = new RecordBatchLoader(allocator);
      final StringBuilder builder = new StringBuilder();

      for (final QueryDataBatch b : results) {
        if (!b.hasData()) {
          continue;
        }

        loader.load(b.getHeader().getDef(), b.getData());

        final VectorWrapper<?> vw;
        try {
            vw = loader.getValueAccessorById(
                NullableVarCharVector.class,
                loader.getValueVectorId(SchemaPath.getSimplePath(columnName)).getFieldIds());
        } catch (Throwable t) {
          throw new IllegalStateException("Looks like you did not provide an explain plan query, please add EXPLAIN PLAN FOR to the beginning of your query.");
        }

        final ValueVector vv = vw.getValueVector();
        for (int i = 0; i < vv.getAccessor().getValueCount(); i++) {
          final Object o = vv.getAccessor().getObject(i);
          builder.append(o);
        }
        loader.clear();
        b.release();
      }

      return builder.toString();
    }
  }

  private DrillConfig config;
  private Drillbit bits[];
  private DrillClient client;
  private static BufferAllocator allocator;
  private RemoteServiceSet serviceSet;
  private String dfsTestTmpSchemaLocation;

  private ClusterFixture( FixtureBuilder  builder ) throws Exception {

    // Create a config

    if ( builder.configResource != null ) {
      config = DrillConfig.create(builder.configResource);
     } else if ( builder.configProps != null ) {
      config = DrillConfig.create(builder.configProps);
    } else {
      config = DrillConfig.create(TEST_CONFIGURATIONS);
    }

    // Not quite sure what this is, but some tests seem to use it.

    if (builder.enableFullCache ||
        (config.hasPath(ENABLE_FULL_CACHE) && config.getBoolean(ENABLE_FULL_CACHE))) {
      serviceSet = RemoteServiceSet.getServiceSetWithFullCache(config, allocator);
    } else {
      serviceSet = RemoteServiceSet.getLocalServiceSet();
    }

    dfsTestTmpSchemaLocation = TestUtilities.createTempDir();

    Preconditions.checkArgument(builder.bitCount > 0);
    bits = new Drillbit[builder.bitCount];
    for ( int i = 0;  i < bits.length;  i++ ) {
      bits[i] = new Drillbit(config, serviceSet);
      bits[i].run( );

      // Create the dfs_test name space

      final StoragePluginRegistry pluginRegistry = bits[i].getContext().getStorage();
      TestUtilities.updateDfsTestTmpSchemaLocation(pluginRegistry, dfsTestTmpSchemaLocation);
      TestUtilities.makeDfsTmpSchemaImmutable(pluginRegistry);

      // Create the mock data plugin

      MockStorageEngineConfig config = MockStorageEngineConfig.INSTANCE;
      MockStorageEngine plugin = new MockStorageEngine(MockStorageEngineConfig.INSTANCE, bits[i].getContext(), MockStorageEngineConfig.NAME);
      ((StoragePluginRegistryImpl) pluginRegistry).definePlugin(MockStorageEngineConfig.NAME, config, plugin);
    }

    // Create a client.

    client = new DrillClient(config, serviceSet.getCoordinator());
    client.connect(builder.clientProps);

    // Some operations need an allocator.

    allocator = RootAllocatorFactory.newRoot(config);

    // Apply session options.

    boolean sawMaxWidth = false;
    if ( builder.runtimeSettings != null ) {
      for ( ClusterFixture.RuntimeOption option : builder.runtimeSettings ) {
        alterSession( option.key, option.value );
        if ( option.key.equals( ExecConstants.MAX_WIDTH_PER_NODE_KEY ) ) {
          sawMaxWidth = true;
        }
      }
    }

    // Set the default parallelization unless already set by the caller.

    if ( ! sawMaxWidth ) {
      alterSession( ExecConstants.MAX_WIDTH_PER_NODE_KEY, MAX_WIDTH_PER_NODE );
    }
  }

  /**
   * Set a runtime option.
   *
   * @param key
   * @param value
   * @throws RpcException
   */

  public void alterSession(String key, Object value ) throws RpcException {
    String sql = "ALTER SESSION SET `" + key + "` = " + stringify( value );
    runSqlSilently( sql );
  }

  private static String stringify(Object value) {
    if ( value instanceof String ) {
      return "'" + (String) value + "'";
    } else {
      return value.toString();
    }
  }

  private static String trimSlash(String path) {
    if ( path == null ) {
      return path;
    } else if ( path.startsWith("/" ) ) {
      return path.substring( 1 );
    } else {
      return path;
    }
  }

  /**
   * Run SQL silently (discard results.)
   *
   * @param sql
   * @throws RpcException
   */

  public void runSqlSilently(String sql) throws RpcException {
    queryBuilder().sql(sql).run();
  }

  public QueryBuilder queryBuilder() {
    return new QueryBuilder();
  }

//  /**
//   * Discard the results returned from a query.
//   *
//   * @param results
//   */
//
//  public void discardResults(List<QueryDataBatch> results) {
//    for (QueryDataBatch queryDataBatch : results) {
//      queryDataBatch.release();
//    }
//  }

//  /**
//   * Run SQL and return the results.
//   *
//   * @param sql
//   * @return
//   * @throws RpcException
//   */
//  public List<QueryDataBatch> runSql(String sql) throws RpcException {
//    return runQuery(QueryType.SQL, sql);
//  }

//  /**
//   * Run SQL stored in a resource file and return the results.
//   *
//   * @param file
//   * @throws Exception
//   */
//
//  public List<QueryDataBatch> runSqlFromResource(String file) {
//    return runSql(loadResource(file));
//  }

  public static String getResource(String resource) throws IOException {
    // Unlike the Java routines, Guava does not like a leading slash.

    final URL url = Resources.getResource(trimSlash(resource));
    if (url == null) {
      throw new IOException(String.format("Unable to find resource %s.", resource));
    }
    return Resources.toString(url, Charsets.UTF_8);
  }

  public static String loadResource(String resource) {
    try {
      return getResource(resource);
    } catch (IOException e) {
      throw new IllegalStateException("Resource not found: " + resource, e);
    }
  }

//  /**
//   * Run a physical plan stored in a resource and return the results.
//   *
//   * @param file
//   * @return
//   * @throws Exception
//   */
//
//  public List<QueryDataBatch> runPhysicalFromResource(String file) {
//    return runQuery(QueryType.PHYSICAL, loadResource(file));
//  }
//
//  /**
//   * Generic method to run a query of the specifed type and return the results.
//   *
//   * @param type
//   * @param query
//   * @return
//   * @throws RpcException
//   */
//
//  public List<QueryDataBatch> runQuery(QueryType type, String query) throws RpcException {
//    return client.runQuery(type, query);
//  }

  public int countResults(List<QueryDataBatch> results) {
    int count = 0;
    for(QueryDataBatch b : results) {
      count += b.getHeader().getRowCount();
    }
    return count;
  }

  public Drillbit drillbit( ) { return bits[0]; }
  public DrillClient client() { return client; }
  public RemoteServiceSet serviceSet( ) { return serviceSet; }
  public BufferAllocator allocator( ) { return allocator; }

  @Override
  public void close() throws Exception {
    Exception ex = safeClose( client, null );
    client = null;
    for (int i = 0; i < bits.length; i++) {
      ex = safeClose( bits[i], ex );
    }
    bits = null;
    ex = safeClose( serviceSet, ex );
    serviceSet = null;
    ex = safeClose( allocator, ex );
    allocator = null;
    if ( ex != null ) {
      throw ex; }
  }

  private Exception safeClose(AutoCloseable item, Exception ex) {
    try {
      if ( item != null ) {
        item.close( ); }
    } catch ( Exception e ) {
      ex = ex == null ? e : ex;
    }
    return ex;
  }

  public void defineWorkspace( String pluginName, String schemaName, String path, String defaultFormat ) throws ExecutionSetupException {
    for ( int i = 0; i < bits.length; i++ ) {
      defineWorkspace(bits[i], pluginName, schemaName, path, defaultFormat);
    }
  }

  public static void defineWorkspace( Drillbit drillbit, String pluginName, String schemaName, String path, String defaultFormat ) throws ExecutionSetupException {
    final StoragePluginRegistry pluginRegistry = drillbit.getContext().getStorage();
    final FileSystemPlugin plugin = (FileSystemPlugin) pluginRegistry.getPlugin(pluginName);
    final FileSystemConfig pluginConfig = (FileSystemConfig) plugin.getConfig();
    final WorkspaceConfig newTmpWSConfig = new WorkspaceConfig(path, true, defaultFormat);

    pluginConfig.workspaces.remove(schemaName);
    pluginConfig.workspaces.put(schemaName, newTmpWSConfig);

    pluginRegistry.createOrUpdate(pluginName, pluginConfig, true);
  }

  public static final String EXPLAIN_PLAN_TEXT = "text";
  public static final String EXPLAIN_PLAN_JSON = "json";

  public static FixtureBuilder builder() {
     return new FixtureBuilder( );
  }

  public TestBuilder testBuilder() {
    return new TestBuilder(allocator);
  }

  public static ClusterFixture standardClient( ) throws Exception {
    return builder( ).build( );
  }
}