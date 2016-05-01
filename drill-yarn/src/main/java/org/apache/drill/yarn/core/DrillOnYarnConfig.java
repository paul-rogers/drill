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
package org.apache.drill.yarn.core;

import java.io.File;
import java.io.PrintStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigValue;

/**
 * Configuration used within the Drill-on-YARN code.
 * Refers to configuration in the user configuration file
 * (drill-on-yarn.conf), the defaults file or overridden
 * by system properties.
 */
public class DrillOnYarnConfig
{
  public static final String DEFAULTS_FILE_NAME = "drill-on-yarn-defaults.conf";
  public static final String CONFIG_FILE_NAME = "drill-on-yarn.conf";

  public static final String DRILL_ON_YARN_PARENT = "drill.yarn";
  public static final String DOY_CLIENT_PARENT = append( DRILL_ON_YARN_PARENT, "client" );
  public static final String DOY_AM_PARENT = append( DRILL_ON_YARN_PARENT, "am" );
  public static final String DOY_DRILLBIT_PARENT = append( DRILL_ON_YARN_PARENT, "drillbit" );
  public static final String DOY_ZK_PARENT = append( DRILL_ON_YARN_PARENT, "zk" );
  public static final String FILES_PARENT = append( DRILL_ON_YARN_PARENT, "drill-install" );
  public static final String DFS_PARENT = append( DRILL_ON_YARN_PARENT, "dfs" );
  public static final String HTTP_PARENT = append( DRILL_ON_YARN_PARENT, "http" );

  public static final String APP_NAME = append( DRILL_ON_YARN_PARENT, "app-name" );
  public static final String CLUSTER_ID = append( DRILL_ON_YARN_PARENT, "cluster-id" );

  public static final String DFS_CONNECTION = append( DFS_PARENT, "connection" );
  public static final String DFS_DIR = append( DFS_PARENT, "dir" );

  public static final String DRILL_ARCHIVE_PATH = append( FILES_PARENT, "client-path" );
  public static final String DRILL_DIR_NAME = append( FILES_PARENT, "dir-name" );

  /**
   * Key used for the Drill archive file in the AM launch config.
   */

  public static final String DRILL_ARCHIVE_KEY = append( FILES_PARENT, "key" );
  public static final String LOCALIZE_DRILL = append( FILES_PARENT, "localize" );
  public static final String DRILL_HOME = append( FILES_PARENT, "drill-home" );

  public static final String MEMORY_KEY = "memory-mb";
  public static final String VCORES_KEY = "vcores";
  public static final String VM_ARGS_KEY = "vm-args";
  public static final String HEAP_KEY = "heap";

  public static final String AM_MEMORY = append( DOY_AM_PARENT, MEMORY_KEY );
  public static final String AM_VCORES = append( DOY_AM_PARENT, VCORES_KEY );
  public static final String AM_HEAP = append( DOY_AM_PARENT, HEAP_KEY );
  public static final String AM_VM_ARGS = append( DOY_AM_PARENT, VM_ARGS_KEY );
  public static final String AM_POLL_PERIOD_MS = append( DOY_AM_PARENT, "poll-ms" );
  public static final String AM_TICK_PERIOD_MS = append( DOY_AM_PARENT, "tick-ms" );
  public static final String AM_CLASSPATH = append( DOY_AM_PARENT, "class-path" );
  public static final String AM_DEBUG_LAUNCH = append( DOY_AM_PARENT, "debug-launch" );

  public static final String DRILLBIT_MEMORY = append( DOY_DRILLBIT_PARENT, MEMORY_KEY );
  public static final String DRILLBIT_VCORES = append( DOY_DRILLBIT_PARENT, VCORES_KEY );
  public static final String DRILLBIT_VM_ARGS = append( DOY_DRILLBIT_PARENT, VM_ARGS_KEY );
  public static final String DRILLBIT_HEAP = append( DOY_DRILLBIT_PARENT, HEAP_KEY );
  public static final String DRILLBIT_DIRECT_MEM = append( DOY_DRILLBIT_PARENT, "max-direct-memory" );
  public static final String DRILLBIT_LOG_GC = append( DOY_DRILLBIT_PARENT, "log-gc" );
  public static final String DRILLBIT_CLASSPATH = append( DOY_DRILLBIT_PARENT, "class-path" );
  public static final String DRILLBIT_MAX_RETRIES = append( DOY_DRILLBIT_PARENT, "max-retries" );
  public static final String DRILLBIT_DEBUG_LAUNCH = append( DOY_DRILLBIT_PARENT, "debug-launch" );
  public static final String DRILLBIT_HTTP_PORT = append( DOY_DRILLBIT_PARENT, "http-port" );

  public static final String ZK_CONNECT = append( DOY_ZK_PARENT, "connect" );
  public static final String ZK_ROOT = append( DOY_ZK_PARENT, "root" );
  public static final String ZK_FAILURE_TIMEOUT_MS = append( DOY_ZK_PARENT, "timeout" );
  public static final String ZK_RETRY_COUNT = append( DOY_ZK_PARENT, "retry.count" );
  public static final String ZK_RETRY_DELAY_MS = append( DOY_ZK_PARENT, "retry.delay" );

  // Names selected to be parallel to Drillbit HTTP config.

  public static final String HTTP_ENABLED = append( HTTP_PARENT, "enabled" );
  public static final String HTTP_PORT = append( HTTP_PARENT, "port" );

  public static final String CLUSTER_POOLS = append( DRILL_ON_YARN_PARENT, "pools" );

  // The following keys are relative to the cluster pool definition

  public static final String POOL_NAME = "name";
  public static final String POOL_TYPE = "type";
  public static final String POOL_SIZE = "count";


  private static DrillOnYarnConfig instance;
  private Config config;

  /**
   * Defined cluster types. The value of the type appears as the
   * value of the {@link $CLUSTER_TYPE} parameter in the config file.
   */

  public enum PoolType {
    BASIC( "basic" ),
    LABELED( "labeled" );

    private String value;

    private PoolType( String value ) {
      this.value = value;
    }

    public static PoolType toEnum( String value ) {
      for ( PoolType type : PoolType.values() ) {
        if ( type.value.equalsIgnoreCase( value ) ) {
          return type; }
      }
      return null;
    }

    public String toValue( ) { return value; }
  }

  public static class Pool
  {
    public String name;
    public int count;
    public PoolType type;

    public void getPairs(int index, List<NameValuePair> pairs) {
      String key = append( CLUSTER_POOLS, Integer.toString( index ) );
      pairs.add( new NameValuePair( append( key, POOL_NAME ), name ) );
      pairs.add( new NameValuePair( append( key, POOL_TYPE ), type ) );
      pairs.add( new NameValuePair( append( key, POOL_SIZE ), count ) );
    }
  }

  public static class BasicPool extends Pool
  {

  }

  public static class LabeledPool extends Pool
  {

  }

  public static String append( String parent, String key ) {
    return parent + "." + key;
  }

  private DrillOnYarnConfig( Config config ) {
    this.config = config;
  }

  public static void load( ) {

    // Resolution order, larger numbers take precedence.
    // 1. Drill-on-YARN defaults.
    // File is at root of the package tree.

    URL url = DrillOnYarnConfig.class.getResource( DEFAULTS_FILE_NAME );
    Config config = null;
    if ( url != null ) {
      config = ConfigFactory.parseURL(url);
    }

    // 2. User's Drill-on-YARN configuration.

    // 3. System properties
    // Allows -Dfoo=bar on the command line.
    // But, note that substitutions are NOT allowed in system properties!

    config = ConfigFactory.load( CONFIG_FILE_NAME ).withFallback( config );

    // Yes, the doc for load( ) seems to imply that the config is resolved.
    // But, it is not really resolved, so do so here.
    // Resolution allows ${foo.bar} syntax in values, but only for values
    // from config files, not from system properties.

    config = config.resolve();
    instance = new DrillOnYarnConfig( config );
  }

  public Config getConfig( ) {
    return instance.config;
  }

  public static DrillOnYarnConfig instance( ) {
    assert instance != null;
    return instance;
  }

  public static Config config( ) {
    return instance( ).getConfig();
  }

  public void dump() {
    dump( System.out );
  }

  private static final String keys[] = {
    APP_NAME,
    CLUSTER_ID,
    DFS_CONNECTION,
    DFS_DIR,
    DRILL_ARCHIVE_PATH,
    DRILL_DIR_NAME,
    DRILL_ARCHIVE_KEY,
    AM_MEMORY,
    AM_VCORES,
    AM_VM_ARGS,
    AM_HEAP,
    AM_POLL_PERIOD_MS,
    AM_TICK_PERIOD_MS,
    AM_CLASSPATH,
    AM_DEBUG_LAUNCH,
    ZK_CONNECT,
    ZK_ROOT,
    ZK_RETRY_COUNT,
    ZK_RETRY_DELAY_MS,
    ZK_FAILURE_TIMEOUT_MS,
    DRILLBIT_MEMORY,
    DRILLBIT_VCORES,
    DRILLBIT_VM_ARGS,
    DRILLBIT_HEAP,
    DRILLBIT_DIRECT_MEM,
    DRILLBIT_CLASSPATH,
    DRILLBIT_MAX_RETRIES,
    DRILLBIT_DEBUG_LAUNCH,
    DRILLBIT_HTTP_PORT,
    HTTP_ENABLED,
    HTTP_PORT
  };

  private void dump(PrintStream out) {
    for ( String key : keys ) {
      out.print( key );
      out.print( " = " );
      try {
        out.println( config.getString( key ) );
      }
      catch ( ConfigException.Missing e ) {
        out.println( "<missing>" );
      }
    }
    out.print( CLUSTER_POOLS );
    out.println( "[" );
    for ( int i = 0;  i < poolCount( );  i++ ) {
      Pool pool = getPool( i );
      out.print( i );
      out.println( " = {" );
      out.print( "  name = " );
      out.println( pool.name );
      out.print( "  type = " );
      out.println( pool.type.toValue() );
      out.print( "  count = " );
      out.println( pool.count );
      out.println( "  }" );
    }
    out.println( "]" );
  }

  public List<NameValuePair> getPairs( ) {
    List<NameValuePair> pairs = new ArrayList<>( );
    for ( String key : keys ) {
      pairs.add( new NameValuePair( key, config.getString( key ) ) );
    }
    for ( int i = 0;  i < poolCount( );  i++ ) {
      Pool pool = getPool( i );
      pool.getPairs( i, pairs );
    }
    return pairs;
  }

  public static String poolKey( int index, String key ) {
    return CLUSTER_POOLS + "." + index + "." + key;
  }

  public int poolCount( ) {
    return config.getList(CLUSTER_POOLS).size();
  }

  private static String suffixes[] = {
      ".tar.gz", ".tgz",".zip" };

  public static String findSuffix( String baseName ) {
    baseName = baseName.toLowerCase();
    for ( String extn : suffixes ) {
      if ( baseName.endsWith( extn ) ) {
        return extn; }
    }
    return null;
  }

  public static String getDrillHome( Config config ) {
    if ( ! config.getBoolean( DrillOnYarnConfig.LOCALIZE_DRILL ) ) {
      return config.getString( DrillOnYarnConfig.DRILL_HOME );
    }
    String home = config.getString( DrillOnYarnConfig.DRILL_DIR_NAME );
    if ( ! DoYUtil.isBlank( home ) ) {
      return home;
    }
    File localArchiveFile = new File( config.getString( DrillOnYarnConfig.DRILL_ARCHIVE_PATH ) );
    home = localArchiveFile.getName( );
    String suffix = findSuffix( home );
    return home.substring( 0, home.length() - suffix.length() );
  }

  public Pool getPool( int n ) {
    ConfigList pools = config.getList(CLUSTER_POOLS);
    ConfigValue value = pools.get( n );
    @SuppressWarnings("unchecked")
    Map<String,Object> pool = (Map<String,Object>) value.unwrapped();
    String type;
    try {
      type = pool.get( POOL_TYPE ).toString();
    }
    catch ( NullPointerException e ) {
      throw new IllegalArgumentException( "Pool type is required for pool " + n );
    }
    PoolType poolType = PoolType.toEnum( type );
    if ( poolType == null ) {
      throw new IllegalArgumentException( "Undefined type for pool " + n + ": " + type );
    }
    Pool poolDef;
    switch ( poolType ) {
    case BASIC:
      poolDef = new BasicPool( );
      break;
    case LABELED:
      poolDef = new LabeledPool( );
      break;
    default:
      assert false;
      throw new IllegalStateException( "Undefined pool type: " + poolType );
    }
    poolDef.type = poolType;
    try {
      poolDef.count = (Integer) pool.get( POOL_SIZE );
    }
    catch ( ClassCastException e ) {
      throw new IllegalArgumentException( "Expected an integer for " + POOL_SIZE + " for pool " + n );
    }
    poolDef.name = pool.get( POOL_NAME ).toString();
    if ( DoYUtil.isBlank( poolDef.name ) ) {
      poolDef.name = "pool-" + Integer.toString( n + 1 );
    }
    return poolDef;
  }
}
