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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.drill.BufferingQueryEventListener.QueryEvent;
import org.apache.drill.DrillTestWrapper.TestServices;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ZookeeperHelper;
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
import org.apache.drill.test.DrillTest;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
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

      put(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, false);
      put(ExecConstants.HTTP_ENABLE, false);
      put(Drillbit.SYSTEM_OPTIONS_NAME, "org.apache.drill.exec.compile.ClassTransformer.scalar_replacement=on");
      put(QueryTestUtil.TEST_QUERY_PRINTING_SILENT, true);
      put("drill.catastrophic_to_standard_out", true);

      // See Drillbit.close. The Drillbit normally waits a specified amount
      // of time for ZK registration to drop. But, embedded Drillbits normally
      // don't use ZK, so no need to wait.

      put(ExecConstants.ZK_REFRESH, 0);

      // This is just a test, no need to be heavy-duty on threads.
      // This is the number of server and client RPC threads. The
      // production default is DEFAULT_SERVER_RPC_THREADS.

      put(ExecConstants.BIT_SERVER_RPC_THREADS, 2);

      // No need for many scanners except when explicitly testing that
      // behavior. Production default is DEFAULT_SCAN_THREADS

      put(ExecConstants.SCAN_THREADPOOL_SIZE, 4);
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

    // Values in the drill-module.conf file for values that are customized
    // in the defaults.

    public static final int DEFAULT_ZK_REFRESH = 500; // ms
    public static final int DEFAULT_SERVER_RPC_THREADS = 10;
    public static final int DEFAULT_SCAN_THREADS = 8;

    public static Properties defaultProps( ) {
      Properties props = new Properties( );
      props.putAll( TEST_CONFIGURATIONS );
      return props;
    }

    private String configResource;
    private Properties configProps;
    private boolean enableFullCache;
    private List<ClusterFixture.RuntimeOption> sessionOptions;
    private List<ClusterFixture.RuntimeOption> systemOptions;
    private int bitCount = 1;
    private String bitNames[];
    private int zkCount;
    private ZookeeperHelper zkHelper;

    /**
     * Use the given configuration properties to start the embedded Drillbit.
     * @param configProps a collection of config properties
     * @return this builder
     * @see {@link #configProperty(String, Object)}
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
     * config settings with {@link #configProperty(String, Object)}.
     * @param configResource path to the file that contains the
     * config file to be read
     * @return this builder
     * @see {@link #configProperty(String, Object)}
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

    public FixtureBuilder configProperty( String key, Object value ) {
      if ( configProps == null ) {
        configProps = defaultProps( );
      }
      configProps.put(key, value.toString());
      return this;
    }

     /**
     * Provide a session option to be set once the Drillbit
     * is started.
     *
     * @param key the name of the session option
     * @param value the value of the session option
     * @return this builder
     * @see {@link ClusterFixture#alterSession(String, Object)}
     */

    public FixtureBuilder sessionOption( String key, Object value ) {
      if ( sessionOptions == null ) {
        sessionOptions = new ArrayList<>( );
      }
      sessionOptions.add( new RuntimeOption( key, value ) );
      return this;
    }

    /**
     * Provide a system option to be set once the Drillbit
     * is started.
     *
     * @param key the name of the system option
     * @param value the value of the system option
     * @return this builder
     * @see {@link ClusterFixture#alterSystem(String, Object)}
     */

    public FixtureBuilder systemOption( String key, Object value ) {
      if ( systemOptions == null ) {
        systemOptions = new ArrayList<>( );
      }
      systemOptions.add( new RuntimeOption( key, value ) );
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
      return sessionOption( ExecConstants.MAX_WIDTH_PER_NODE_KEY, n );
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
      bitNames = null;
      return this;
    }

    /**
     * Define a cluster by providing names to the Drillbits.
     * The cluster size is the same as the number of names provided.
     *
     * @param bitNames array of (unique) Drillbit names
     * @return this builder
     */
    public FixtureBuilder withBits(String bitNames[]) {
      this.bitNames = bitNames;
      bitCount = bitNames.length;
      return this;
    }

    /**
     * By default the embedded Drillbits use an in-memory cluster coordinator.
     * Use this option to start an in-memory ZK instance to coordinate the
     * Drillbits.
     * @return this builder
     */
    public FixtureBuilder withZk() {
      return withZk(1);
    }

    public FixtureBuilder withZk(int count) {
      zkCount = count;

      // Using ZK. Turn refresh wait back on.

      configProperty(ExecConstants.ZK_REFRESH, DEFAULT_ZK_REFRESH);
      return this;
    }

    /**
     * Run the cluster using a Zookeeper started externally. Use this if
     * multiple tests start a cluster: allows ZK to be started once for
     * the entire suite rather than once per test case.
     *
     * @param zk the global Zookeeper to use
     * @return this builder
     */
    public FixtureBuilder withZk(ZookeeperHelper zk) {
      zkHelper = zk;

      // Using ZK. Turn refresh wait back on.

      configProperty(ExecConstants.ZK_REFRESH, DEFAULT_ZK_REFRESH);
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

  /**
   * Summary results of a query: records, batches, run time.
   */

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

  /**
   * Builder for a Drill query. Provides all types of query formats,
   * and a variety of ways to run the query.
   */

  public static class QueryBuilder {

    private final ClientFixture client;
    private QueryType queryType;
    private String queryText;

    private QueryBuilder(ClientFixture client) {
      this.client = client;
    }

    public QueryBuilder query(QueryType type, String text) {
      queryType = type;
      queryText = text;
      return this;
    }

    public QueryBuilder sql(String sql) {
      return query( QueryType.SQL, sql );
    }

    public QueryBuilder sql(String query, Object... args) {
      return sql(String.format(query, args));
    }

    public QueryBuilder physical(String plan) {
      return query( QueryType.PHYSICAL, plan);
    }

    public QueryBuilder sqlResource(String resource) {
      sql(loadResource(resource));
      return this;
    }

    public QueryBuilder sqlResource(String resource, Object... args) {
      sql(loadResource(resource), args);
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
      return client.client().runQuery(queryType, queryText);
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
      client.client().runQuery(queryType, queryText, listener);
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

    public long printCsv() {
      return print(Format.CSV);
    }

    public long print( Format format ) {
      return print(format,20);
    }

    public long print(Format format, int colWidth) {
      return runAndWait( new PrintingResultsListener( client.cluster.config( ), format, colWidth ) );
    }

    /**
     * Run a query and optionally print the output in TSV format.
     * Similar to {@link QueryTestUtil#test} with one query. Output is printed
     * only if the tests are running as verbose.
     *
     * @return the number of rows returned
     */
    public long print() {
      DrillConfig config = client.cluster.config( );
      boolean verbose = ! config.getBoolean(QueryTestUtil.TEST_QUERY_PRINTING_SILENT) ||
                        DrillTest.verbose();
      if (verbose) {
        return print(Format.TSV, VectorUtil.DEFAULT_COLUMN_WIDTH);
      } else {
        return run().recordCount();
      }
    }

    public long runAndWait(UserResultsListener listener) {
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
          throw event.error;
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
      final RecordBatchLoader loader = new RecordBatchLoader(client.allocator( ));
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

        @SuppressWarnings("resource")
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

  public static class ClientBuilder {

    ClusterFixture cluster;
    private Properties clientProps;

    protected ClientBuilder(ClusterFixture cluster) {
      this.cluster = cluster;
    }
    /**
     * Specify an optional client property.
     * @param key property name
     * @param value property value
     * @return this builder
     */
    public ClientBuilder property( String key, Object value ) {
      if ( clientProps == null ) {
        clientProps = new Properties( );
      }
      clientProps.put(key, value);
      return this;
    }

    ClientFixture build( ) {
      try {
        return new ClientFixture(this);
      } catch (RpcException e) {

        // When used in a test with an embedded Drillbit, the
        // RPC exception should not occur.

        throw new IllegalStateException(e);
      }
    }
  }

  public static class ClientFixture implements AutoCloseable {

    private ClusterFixture cluster;
    private DrillClient client;

    public ClientFixture(ClientBuilder builder) throws RpcException {
      this.cluster = builder.cluster;

      // Create a client.

      client = new DrillClient(cluster.config( ), cluster.serviceSet( ).getCoordinator());
      client.connect(builder.clientProps);
      cluster.clients.add(this);
    }

    public DrillClient client() { return client; }
    public ClusterFixture cluster( ) { return cluster; }
    public BufferAllocator allocator( ) { return cluster.allocator( ); }

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

    public void alterSystem(String key, Object value ) throws RpcException {
      String sql = "ALTER SYSTEM SET `" + key + "` = " + stringify( value );
      runSqlSilently( sql );
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
      return new QueryBuilder(this);
    }

    public int countResults(List<QueryDataBatch> results) {
      int count = 0;
      for(QueryDataBatch b : results) {
        count += b.getHeader().getRowCount();
      }
      return count;
    }

    public TestBuilder testBuilder() {
      return new TestBuilder(new FixtureTestServices(this));
    }

    /**
     * Run zero or more queries and optionally print the output in TSV format.
     * Similar to {@link QueryTestUtil#test}. Output is printed
     * only if the tests are running as verbose.
     *
     * @return the number of rows returned
     */

    public void runQueries(final String queryString) throws Exception{
      final String query = QueryTestUtil.normalizeQuery(queryString);
      String[] queries = query.split(";");
      for (String q : queries) {
        final String trimmedQuery = q.trim();
        if (trimmedQuery.isEmpty()) {
          continue;
        }
        queryBuilder( ).sql(trimmedQuery).print();
      }
    }

    @Override
    public void close( ) {
      if (client == null) {
        return;
      }
      try {
        client.close( );
      } finally {
        client = null;
        cluster.clients.remove(this);
      }
    }
  }

  public static final String DEFAULT_BIT_NAME = "drillbit";

  private DrillConfig config;
  private Map<String,Drillbit> bits = new HashMap<>( );
  private BufferAllocator allocator;
  private boolean ownsZK;
  private ZookeeperHelper zkHelper;
  private RemoteServiceSet serviceSet;
  private String dfsTestTmpSchemaLocation;
  private List<ClientFixture> clients = new ArrayList<>( );

  private ClusterFixture( FixtureBuilder  builder ) throws Exception {

    // Start ZK if requested.

    if (builder.zkHelper != null) {
      zkHelper = builder.zkHelper;
      ownsZK = false;
    } else if (builder.zkCount > 0) {
      zkHelper = new ZookeeperHelper(true);
      zkHelper.startZookeeper(builder.zkCount);
      ownsZK = true;
    }

    // Create a config
    // Because of the way DrillConfig works, we can set the ZK
    // connection string only if a property set is provided.

    if ( builder.configResource != null ) {
      config = DrillConfig.create(builder.configResource);
    } else if (builder.configProps != null) {
      config = DrillConfig.create(configProperties(builder.configProps));
    } else {
      config = DrillConfig.create(configProperties(TEST_CONFIGURATIONS));
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
    int bitCount = builder.bitCount;
    for ( int i = 0;  i < bitCount;  i++ ) {
      @SuppressWarnings("resource")
      Drillbit bit = new Drillbit(config, serviceSet);
      bit.run( );

      // Create the dfs_test name space

      @SuppressWarnings("resource")
      final StoragePluginRegistry pluginRegistry = bit.getContext().getStorage();
      TestUtilities.updateDfsTestTmpSchemaLocation(pluginRegistry, dfsTestTmpSchemaLocation);
      TestUtilities.makeDfsTmpSchemaImmutable(pluginRegistry);

      // Create the mock data plugin

      MockStorageEngineConfig config = MockStorageEngineConfig.INSTANCE;
      @SuppressWarnings("resource")
      MockStorageEngine plugin = new MockStorageEngine(MockStorageEngineConfig.INSTANCE, bit.getContext(), MockStorageEngineConfig.NAME);
      ((StoragePluginRegistryImpl) pluginRegistry).definePlugin(MockStorageEngineConfig.NAME, config, plugin);

      // Bit name and registration.

      String name;
      if (builder.bitNames != null && i < builder.bitNames.length) {
        name = builder.bitNames[i];
      } else {
        if (i == 0) {
          name = DEFAULT_BIT_NAME;
        } else {
          name = DEFAULT_BIT_NAME + Integer.toString(i+1);
        }
      }
      bits.put(name, bit);
    }

    // Some operations need an allocator.

    allocator = RootAllocatorFactory.newRoot(config);

    // Apply system options
    if ( builder.systemOptions != null ) {
      for ( ClusterFixture.RuntimeOption option : builder.systemOptions ) {
        clientFixture( ).alterSystem( option.key, option.value );
      }
    }
    // Apply session options.

    boolean sawMaxWidth = false;
    if ( builder.sessionOptions != null ) {
      for ( ClusterFixture.RuntimeOption option : builder.sessionOptions ) {
        clientFixture( ).alterSession( option.key, option.value );
        if ( option.key.equals( ExecConstants.MAX_WIDTH_PER_NODE_KEY ) ) {
          sawMaxWidth = true;
        }
      }
    }

    // Set the default parallelization unless already set by the caller.

    if ( ! sawMaxWidth ) {
      clientFixture( ).alterSession( ExecConstants.MAX_WIDTH_PER_NODE_KEY, MAX_WIDTH_PER_NODE );
    }
  }

  private Properties configProperties(Properties configProps) {
    Properties effectiveProps = new Properties( );
    for (Entry<Object, Object> entry : configProps.entrySet()) {
      effectiveProps.put(entry.getKey(), entry.getValue().toString());
    }
    if (zkHelper != null) {
      effectiveProps.put(ExecConstants.ZK_CONNECTION, zkHelper.getConfig().getString(ExecConstants.ZK_CONNECTION));
    }
    return effectiveProps;
  }

  public Drillbit drillbit( ) { return bits.get(DEFAULT_BIT_NAME); }
  public Drillbit drillbit(String name) { return bits.get(name); }
  public Collection<Drillbit> drillbits( ) { return bits.values(); }
  public RemoteServiceSet serviceSet( ) { return serviceSet; }
  public BufferAllocator allocator( ) { return allocator; }
  public DrillConfig config() { return config; }

  public ClientBuilder clientBuilder( ) {
    return new ClientBuilder(this);
  }

  public ClientFixture clientFixture() {
    if ( clients.isEmpty( ) ) {
      clientBuilder( ).build( );
    }
    return clients.get(0);
  }

  public DrillClient client() {
    return clientFixture().client();
  }

  @Override
  public void close() throws Exception {
    Exception ex = null;

    // Close clients. Clients remove themselves from the client
    // list.

    while (! clients.isEmpty( )) {
      ex = safeClose(clients.get(0), ex);
    }

    for (Drillbit bit : drillbits()) {
      ex = safeClose(bit, ex);
    }
    bits.clear();
    ex = safeClose(serviceSet, ex);
    serviceSet = null;
    ex = safeClose(allocator, ex);
    allocator = null;
    if (zkHelper != null && ownsZK) {
      try {
        zkHelper.stopZookeeper();
      } catch (Exception e) {
        ex = ex == null ? e : ex;
      }
    }
    zkHelper = null;
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
    for (Drillbit bit : drillbits()) {
      defineWorkspace(bit, pluginName, schemaName, path, defaultFormat);
    }
  }

  public static void defineWorkspace( Drillbit drillbit, String pluginName, String schemaName, String path, String defaultFormat ) throws ExecutionSetupException {
    @SuppressWarnings("resource")
    final StoragePluginRegistry pluginRegistry = drillbit.getContext().getStorage();
    @SuppressWarnings("resource")
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

  public static class FixtureTestServices implements TestServices {

    private ClientFixture client;

    public FixtureTestServices(ClientFixture client) {
      this.client = client;
    }

    @Override
    public BufferAllocator allocator() {
      return client.allocator();
    }

    @Override
    public void test(String query) throws Exception {
      client.runQueries(query);
    }

    @Override
    public List<QueryDataBatch> testRunAndReturn(QueryType type, Object query)
        throws Exception {
      return client.queryBuilder().query(type, (String) query).results( );
    }
  }

  public static ClusterFixture standardCluster( ) throws Exception {
    return builder( ).build( );
  }

  private static String stringify(Object value) {
    if ( value instanceof String ) {
      return "'" + (String) value + "'";
    } else {
      return value.toString();
    }
  }

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
   * Create a temp directory to store the given <i>dirName</i>.
   * Directory will be deleted on exit. Directory is created if it does
   * not exist.
   * @param dirName directory name
   * @return Full path including temp parent directory and given directory name.
   */
  public static File getTempDir(final String dirName) {
    final File dir = Files.createTempDir();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        FileUtils.deleteQuietly(dir);
      }
    });
    File tempDir = new File( dir, dirName );
    tempDir.mkdirs();
    return tempDir;
  }
}