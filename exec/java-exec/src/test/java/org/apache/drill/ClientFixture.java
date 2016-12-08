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

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.util.TestUtilities;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

/**
 * Test fixture to start a Drillbit with provide options, create a client,
 * and execute queries. Can be used in JUnit tests, or in ad-hoc programs.
 * Provides a builder to set the necessary embedded Drillbit and client
 * options, then creates the requested Drillbit and client.
 */

public class ClientFixture implements AutoCloseable {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ClientFixture.class);
  private static final String ENABLE_FULL_CACHE = "drill.exec.test.use-full-cache";
  private static final int MAX_WIDTH_PER_NODE = 2;

  @SuppressWarnings("serial")
  protected static final Properties TEST_CONFIGURATIONS = new Properties() {
    {
      put(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, "false");
      put(ExecConstants.HTTP_ENABLE, "false");
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
    private List<ClientFixture.RuntimeOption> runtimeSettings;

    /**
     * Use the given configuration properties to start the embedded Drillbit.
     * @param configProps
     * @return
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
     * @param configResource
     * @return
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
     * @param key
     * @param value
     * @return
     */

    public FixtureBuilder property( String key, Object value ) {
      if ( configProps == null ) {
        configProps = defaultProps( );
      }
      configProps.put(key, stringify(value));
      return this;
    }

    /**
     * Specify an optional client property.
     * @param key
     * @param value
     * @return
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
     * @param key
     * @param value
     * @return
     * @see {@link ClientFixture#alterSession(String, Object)}
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
     * @param n
     * @return
     */
    public FixtureBuilder maxParallelization(int n) {
      return option( ExecConstants.MAX_WIDTH_PER_NODE_KEY, n );
    }

    public FixtureBuilder enableFullCache( ) {
      enableFullCache = true;
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
    public ClientFixture build( ) throws Exception {
      return new ClientFixture( this );
    }
  }

  private Drillbit bit;
  private DrillClient client;
  private static BufferAllocator allocator;
  private RemoteServiceSet serviceSet;
  private String dfsTestTmpSchemaLocation;

  private ClientFixture( FixtureBuilder  builder ) throws Exception {

    // Create a config

    DrillConfig config;
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

    bit = new Drillbit(config, serviceSet);
    bit.run( );

    // Create the dfs_test name space

    dfsTestTmpSchemaLocation = TestUtilities.createTempDir();
    final StoragePluginRegistry pluginRegistry = bit.getContext().getStorage();
    TestUtilities.updateDfsTestTmpSchemaLocation(pluginRegistry, dfsTestTmpSchemaLocation);
    TestUtilities.makeDfsTmpSchemaImmutable(pluginRegistry);

    // Create a client.

    client = new DrillClient(config, serviceSet.getCoordinator());
    client.connect(builder.clientProps);

    // Some operations need an allocator.

    allocator = RootAllocatorFactory.newRoot(config);

    // Apply session options.

    boolean sawMaxWidth = false;
    if ( builder.runtimeSettings != null ) {
      for ( ClientFixture.RuntimeOption option : builder.runtimeSettings ) {
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
    discardResults( runSql(sql) );
  }

  /**
   * Discard the results returned from a query.
   *
   * @param results
   */

  public void discardResults(List<QueryDataBatch> results) {
    for (QueryDataBatch queryDataBatch : results) {
      queryDataBatch.release();
    }
  }

  /**
   * Run SQL and return the results.
   *
   * @param sql
   * @return
   * @throws RpcException
   */
  public List<QueryDataBatch> runSql(String sql) throws RpcException {
    return runQuery(QueryType.SQL, sql);
  }

  /**
   * Run SQL stored in a resource file and return the results.
   *
   * @param file
   * @throws Exception
   */

  public List<QueryDataBatch> runSqlFromResource(String file) throws Exception{
    return runSql(getResource(file));
  }

  public static String getResource(String resource) throws IOException{
    // Unlike the Java routines, Guava does not like a leading slash.

    final URL url = Resources.getResource(trimSlash(resource));
    if (url == null) {
      throw new IOException(String.format("Unable to find resource %s.", resource));
    }
    return Resources.toString(url, Charsets.UTF_8);
  }

  /**
   * Run a physical plan stored in a resource and return the results.
   *
   * @param file
   * @return
   * @throws Exception
   */

  public List<QueryDataBatch> runPhysicalFromResource(String file) throws Exception {
    return runQuery(QueryType.PHYSICAL, getResource(file));
  }

  /**
   * Generic method to run a query of the specifed type and return the results.
   *
   * @param type
   * @param query
   * @return
   * @throws RpcException
   */

  public List<QueryDataBatch> runQuery(QueryType type, String query) throws RpcException {
    return client.runQuery(type, query);
  }

  public int countResults(List<QueryDataBatch> results) {
    int count = 0;
    for(QueryDataBatch b : results) {
      count += b.getHeader().getRowCount();
    }
    return count;
  }

  public Drillbit drillbit( ) { return bit; }
  public DrillClient client() { return client; }
  public RemoteServiceSet serviceSet( ) { return serviceSet; }
  public BufferAllocator allocator( ) { return allocator; }

  @Override
  public void close() throws Exception {
    Exception ex = safeClose( client, null );
    client = null;
    ex = safeClose( bit, ex );
    bit = null;
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

  public static FixtureBuilder builder() {
     return new FixtureBuilder( );
  }
}