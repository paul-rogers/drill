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
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

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
  public static final String YARN_PARENT = append( DRILL_ON_YARN_PARENT, "yarn" );

  public static final String APP_NAME = append( DRILL_ON_YARN_PARENT, "app-name" );
  public static final String CLUSTER_ID = append( DRILL_ON_YARN_PARENT, "cluster-id" );

  public static final String DFS_CONNECTION = append( DFS_PARENT, "connection" );
  public static final String DFS_APP_DIR = append( DFS_PARENT, "app-dir" );
//  public static final String DFS_CONFIG_DIR = append( DFS_PARENT, "config-dir" );
//  public static final String DFS_USE_HADOOP_CONFIG = append( DFS_PARENT, "use-hadoop-config" );

  public static final String YARN_QUEUE = append( YARN_PARENT, "queue" );
  public static final String YARN_PRIORITY = append( YARN_PARENT, "priority" );

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
  public static final String AM_PREFIX_CLASSPATH = append( DOY_AM_PARENT, "prefix-class-path" );
  public static final String AM_CLASSPATH = append( DOY_AM_PARENT, "class-path" );
  public static final String AM_DEBUG_LAUNCH = append( DOY_AM_PARENT, "debug-launch" );

  public static final String DRILLBIT_MEMORY = append( DOY_DRILLBIT_PARENT, MEMORY_KEY );
  public static final String DRILLBIT_VCORES = append( DOY_DRILLBIT_PARENT, VCORES_KEY );
  public static final String DRILLBIT_VM_ARGS = append( DOY_DRILLBIT_PARENT, VM_ARGS_KEY );
  public static final String DRILLBIT_HEAP = append( DOY_DRILLBIT_PARENT, HEAP_KEY );
  public static final String DRILLBIT_DIRECT_MEM = append( DOY_DRILLBIT_PARENT, "max-direct-memory" );
  public static final String DRILLBIT_LOG_GC = append( DOY_DRILLBIT_PARENT, "log-gc" );
  public static final String DRILLBIT_PREFIX_CLASSPATH = append( DOY_DRILLBIT_PARENT, "prefix-class-path" );
  public static final String DRILLBIT_EXTN_CLASSPATH = append( DOY_DRILLBIT_PARENT, "extn-class-path" );
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
  private static File drillHome;
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

  public static void load( ) throws DoyConfigException
  {
    loadConfig( );
    instance.setDrillHome( );
  }

  private static void loadConfig( )
  {
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

  /**
   * Infer Drill home from the location of the DoY config file. This has
   * the secondary effect of validating that the config file exists in
   * the expected location.
   * <p>
   * Note: This structure does not allow configs to appear anywhere EXCEPT
   * in $DRILL_HOME/conf. That restriction is necessary for DoY as only the
   * $DRILL_HOME folder is localized to each YARN node. Revisit this assumption
   * (and the Drill home calculation) of we allow a separate config folder.
   *
   * @throws DoyConfigException
   */

  private void setDrillHome( ) throws DoyConfigException
  {
    String homeDir = instance.config.getString( DRILL_HOME );
    if ( ! DoYUtil.isBlank( homeDir ) ) {
      drillHome = new File( homeDir );
      return;
    }

    // If we get this far, then we do have a config file. Resolve that
    // file again and use it to infer the location of DRILL_HOME.

    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = DrillOnYarnConfig.class.getClassLoader();
    }
    URL url = classLoader.getResource( CONFIG_FILE_NAME );
    if ( url == null ) {
      throw new DoyConfigException( "Drill-on-YARN configuration file is missing: " + CONFIG_FILE_NAME );
    }
    File confFile;
    try {
      java.nio.file.Path confPath = Paths.get( url.toURI() );
      confFile = confPath.toFile( );
    } catch (URISyntaxException e) {
      throw new DoyConfigException( "Invalid path to Drill-on-YARN configuration file: " + url.toString(), e );
    }
    File parent = confFile.getParentFile();
    if ( parent == null  ||  ! parent.getName().equals( "conf" ) ) {
      throw new DoyConfigException( "The Drill-on-YARN configuration file must reside in the $DRILL_HOME/conf directory: " + confFile.toString() );
    }
    drillHome = parent.getParentFile();
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

  /**
   * Return the Drill home on this machine as inferred from the
   * config file contents or location.
   *
   * @return
   */

  public File getLocalDrillHome( ) { return drillHome; }

  public void dump() {
    dump( System.out );
  }

  private static final String keys[] = {
    APP_NAME,
    CLUSTER_ID,
    DFS_CONNECTION,
    DFS_APP_DIR,
//    DFS_CONFIG_DIR,
//    DFS_USE_HADOOP_CONFIG,
    YARN_QUEUE,
    YARN_PRIORITY,
    DRILL_ARCHIVE_PATH,
    DRILL_DIR_NAME,
    DRILL_ARCHIVE_KEY,
    AM_MEMORY,
    AM_VCORES,
    AM_VM_ARGS,
    AM_HEAP,
    AM_POLL_PERIOD_MS,
    AM_TICK_PERIOD_MS,
    AM_PREFIX_CLASSPATH,
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
    DRILLBIT_PREFIX_CLASSPATH,
    DRILLBIT_EXTN_CLASSPATH,
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

  /**
   * Get the location of Drill home on a remote machine; used when
   * constructing a launch context. Assumes either the absolute path
   * from the config file, or a constructed path to the localized
   * Drill on the remote node.
   *
   * @param config
   * @return
   */

  public static String getRemoteDrillHome( Config config ) {
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
