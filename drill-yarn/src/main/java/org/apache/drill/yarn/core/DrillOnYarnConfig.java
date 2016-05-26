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
  public static final String HADOOP_PARENT = append( DRILL_ON_YARN_PARENT, "hadoop" );
  public static final String CLIENT_PARENT = append( DRILL_ON_YARN_PARENT, "client" );

  public static final String APP_NAME = append( DRILL_ON_YARN_PARENT, "app-name" );
  public static final String CLUSTER_ID = append( DRILL_ON_YARN_PARENT, "cluster-id" );

  public static final String DFS_CONNECTION = append( DFS_PARENT, "connection" );
  public static final String DFS_APP_DIR = append( DFS_PARENT, "app-dir" );

  public static final String YARN_QUEUE = append( YARN_PARENT, "queue" );
  public static final String YARN_PRIORITY = append( YARN_PARENT, "priority" );

  public static final String DRILL_ARCHIVE_PATH = append( FILES_PARENT, "client-path" );
  public static final String DRILL_DIR_NAME = append( FILES_PARENT, "dir-name" );

  /**
   * Key used for the Drill archive file in the AM launch config.
   */

  public static final String DRILL_ARCHIVE_KEY = append( FILES_PARENT, "drill-key" );
  public static final String SITE_ARCHIVE_KEY = append( FILES_PARENT, "site-key" );
  public static final String LOCALIZE_DRILL = append( FILES_PARENT, "localize" );
  public static final String DRILL_HOME = append( FILES_PARENT, "drill-home" );
  public static final String SITE_DIR = append( FILES_PARENT, "site-dir" );

  public static final String HADOOP_HOME = append( HADOOP_PARENT, "home" );
  public static final String HADOOP_CLASSPATH = append( HADOOP_PARENT, "class-path" );
  public static final String HBASE_CLASSPATH = append( HADOOP_PARENT, "hbase-class-path" );

  public static final String MEMORY_KEY = "memory-mb";
  public static final String VCORES_KEY = "vcores";
  public static final String DISKS_KEY = "disks";
  public static final String VM_ARGS_KEY = "vm-args";
  public static final String HEAP_KEY = "heap";

  public static final String AM_MEMORY = append( DOY_AM_PARENT, MEMORY_KEY );
  public static final String AM_VCORES = append( DOY_AM_PARENT, VCORES_KEY );
  public static final String AM_DISKS = append( DOY_AM_PARENT, DISKS_KEY );
  public static final String AM_HEAP = append( DOY_AM_PARENT, HEAP_KEY );
  public static final String AM_VM_ARGS = append( DOY_AM_PARENT, VM_ARGS_KEY );
  public static final String AM_POLL_PERIOD_MS = append( DOY_AM_PARENT, "poll-ms" );
  public static final String AM_TICK_PERIOD_MS = append( DOY_AM_PARENT, "tick-ms" );
  public static final String AM_PREFIX_CLASSPATH = append( DOY_AM_PARENT, "prefix-class-path" );
  public static final String AM_CLASSPATH = append( DOY_AM_PARENT, "class-path" );
  public static final String AM_DEBUG_LAUNCH = append( DOY_AM_PARENT, "debug-launch" );

  public static final String DRILLBIT_MEMORY = append( DOY_DRILLBIT_PARENT, MEMORY_KEY );
  public static final String DRILLBIT_VCORES = append( DOY_DRILLBIT_PARENT, VCORES_KEY );
  public static final String DRILLBIT_DISKS = append( DOY_DRILLBIT_PARENT, DISKS_KEY );
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
  public static final String DISABLE_YARN_LOGS = append( DOY_DRILLBIT_PARENT, "disable-yarn-logs" );

  public static final String ZK_CONNECT = append( DOY_ZK_PARENT, "connect" );
  public static final String ZK_ROOT = append( DOY_ZK_PARENT, "root" );
  public static final String ZK_FAILURE_TIMEOUT_MS = append( DOY_ZK_PARENT, "timeout" );
  public static final String ZK_RETRY_COUNT = append( DOY_ZK_PARENT, "retry.count" );
  public static final String ZK_RETRY_DELAY_MS = append( DOY_ZK_PARENT, "retry.delay" );

  // Names selected to be parallel to Drillbit HTTP config.

  public static final String HTTP_ENABLED = append( HTTP_PARENT, "enabled" );
  public static final String HTTP_PORT = append( HTTP_PARENT, "port" );
  public static final String HTTP_REST_KEY = append( HTTP_PARENT, "rest-key" );

  public static final String CLIENT_POLL_SEC = append( CLIENT_PARENT, "poll-sec" );
  public static final String CLIENT_START_WAIT_SEC = append( CLIENT_PARENT, "start-wait-sec" );
  public static final String CLIENT_STOP_WAIT_SEC = append( CLIENT_PARENT, "stop-wait-sec" );

  public static final String CLUSTER_POOLS = append( DRILL_ON_YARN_PARENT, "pools" );

  // The following keys are relative to the cluster pool definition

  public static final String POOL_NAME = "name";
  public static final String POOL_TYPE = "type";
  public static final String POOL_SIZE = "count";

  /**
   * Name of the subdirectory of the container directory that will hold
   * localized Drill distribution files. This name must be consistent between
   * AM launch request and AM launch, and between Drillbit launch request and
   * Drillbit launch. This name is fixed; there is no reason for the user to
   * change it as it is visible only in the YARN container environment.
   */

  public static String LOCAL_DIR_NAME = "drill";

  public static final String APP_ID_ENV_VAR = "DRILL_AM_APP_ID";
//  public static final String RM_TRACKING_ENV_VAR = "YARN_RM_APP_URL";
  public static final String DRILL_ARCHIVE_ENV_VAR = "DRILL_ARCHIVE";
  public static final String SITE_ARCHIVE_ENV_VAR = "SITE_ARCHIVE";
//  public static final String SITE_DIR_ENV_VAR = "DRILL_SITE_DIR";
//  public static final String SITE_DIR_NAME_ENV_VAR = "DRILL_SITE_DIR_NAME";
  public static final String DRILL_HOME_ENV_VAR = "DRILL_HOME";
  public static final String DRILL_SITE_ENV_VAR = "DRILL_CONF_DIR";
  public static final String AM_HEAP_ENV_VAR = "DRILL_AM_HEAP";
  public static final String AM_JAVA_OPTS_ENV_VAR = "DRILL_AM_JAVA_OPTS";
  public static final String DRILL_CLASSPATH_ENV_VAR = "DRILL_CLASSPATH";
  public static final String DRILL_CLASSPATH_PREFIX_ENV_VAR = "DRILL_CLASSPATH_PREFIX";
  public static final String DRILL_DEBUG_ENV_VAR = "DRILL_DEBUG";

  /**
   * Special value for the DRILL_DIR_NAME parameter to indicate to use
   * the base name of the archive as the Drill home path.
   */

  private static final Object BASE_NAME_MARKER = "<base>";

  /**
   * The name of the Drill site archive stored in dfs. Since the
   * archive is created by the client as a temp file, it's local name
   * has no meaning; we use this standard name on dfs.
   */

  public static final String SITE_ARCHIVE_NAME = "site.tar.gz";

  private static DrillOnYarnConfig instance;
  private static File drillSite;
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

  public static DrillOnYarnConfig load( ) throws DoyConfigException
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
    return instance;
  }

  /**
   * Obtain Drill home from the DRILL_HOME environment variable set by
   * drill-config.sh, which is called from drill-on-yarn.sh. When
   * debugging, DRILL_HOME must be set in the environment.
   * <p>
   * This information is required only by the client to prepare for
   * uploads to DFS.
   *
   * @throws DoyConfigException
   */

  public void setClientPaths( ) throws DoyConfigException
  {
    setClientDrillHome( );
    setSiteDir( );
  }

  private void setClientDrillHome( ) throws DoyConfigException
  {
    // Try the environment variable that should have been
    // set in drill-on-yarn.sh (for the client) or in the
    // launch environment (for the AM.)

    String homeDir = System.getenv( "DRILL_HOME" );

    // For ease in debugging, allow setting the Drill home in drill-on-yarn.conf.
    // This setting is also used for a non-localized run.

    if ( DoYUtil.isBlank( homeDir ) ) {
      homeDir = instance.config.getString( DRILL_HOME );
    }
    if ( DoYUtil.isBlank( homeDir ) ) {
      throw new DoyConfigException( "The DRILL_HOME environment variable must point to your Drill install.");
    }
    drillHome = new File( homeDir );
  }

  /**
   * On both the client and the AM, the site directory is optional.
   * If provided, it was set with the --config (or --site) option to
   * the script that launched the client or AM. In both cases, the
   * script sets the drill.yarn.siteDir system property (and leaks
   * the DRILL_HOME environment variable.)
   * <p>
   * For ease of debugging, if neither of those are set, this method
   * uses the location of the drill-on-yarn configuration file to infer
   * the site directory.
   * <p>
   * On the client, the site directory will be the "original" directory
   * that contains the user's "master" files. On the AM, the site directory
   * is a localized version of the client directory. Because of the way
   * tar works, both the client and AM site directories have the same
   * name; though the path to that name obviously differs.
   *
   * @throws DoyConfigException
   */

  private void setSiteDir( ) throws DoyConfigException
  {
    // The site directory is the one where the config file lives.
    // This should have been set in an environment variable or
    // system property by the launch script.

    String sitePath = System.getProperty( "drill.yarn.siteDir" );
    if ( ! DoYUtil.isBlank( sitePath ) ) {
      drillSite = new File( sitePath );
    }
    else {
      sitePath = System.getenv( "DRILL_CONF_DIR" );
      if ( ! DoYUtil.isBlank( sitePath ) ) {
        drillSite = new File( sitePath );
      }
      else {

        // Otherwise, let's guess it from the config file. This version assists
        // in debugging as it reduces setup steps.

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
        drillSite = confFile.getParentFile();
      }
    }

    // Verify that the site directory is not just $DRILL_HOME/conf.
    // Since the calling script does not differentiate between the two cases.

    if ( drillHome.equals( drillSite.getParentFile() ) ) {
      drillSite = null;
    }
  }

  /**
   * Retrieve the AM Drill home location from the DRILL_HOME variable
   * set in the drill-am.sh launch script.
   *
   * @throws DoyConfigException
   */

  public void setAmDrillHome( ) throws DoyConfigException
  {
    String drillHomeStr = System.getenv( DRILL_HOME_ENV_VAR );
    drillHome = new File( drillHomeStr );
//    String siteDirStr = System.getenv( DRILL_SITE_ENV_VAR );
//    if ( ! DoYUtil.isBlank( siteDirStr ) ) {
//      drillSite = new File( siteDirStr );
//    }
//
//    // Should have been done by the client, but check if the
//    // site directory is in the default location.
//
//    if ( drillHome.equals( drillSite.getParentFile() ) ) {
//      drillSite = null;
//    }
    setSiteDir( );
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
    // drill.yarn

    APP_NAME,
    CLUSTER_ID,

    // drill.yarn.dfs

    DFS_CONNECTION,
    DFS_APP_DIR,

    // drill.yarn.hadoop

    HADOOP_HOME,
    HADOOP_CLASSPATH,
    HBASE_CLASSPATH,

    // drill.yarn.yarn

    YARN_QUEUE,
    YARN_PRIORITY,

    // drill.yarn.drill-install

    DRILL_ARCHIVE_PATH,
    DRILL_DIR_NAME,
    LOCALIZE_DRILL,
    DRILL_HOME,
    DRILL_ARCHIVE_KEY,
    SITE_ARCHIVE_KEY,

    // drill.yarn.client

    CLIENT_POLL_SEC,
    CLIENT_START_WAIT_SEC,
    CLIENT_STOP_WAIT_SEC,

    // drill.yarn.am

    AM_MEMORY,
    AM_VCORES,
    AM_DISKS,
    AM_VM_ARGS,
    AM_HEAP,
    AM_POLL_PERIOD_MS,
    AM_TICK_PERIOD_MS,
    AM_PREFIX_CLASSPATH,
    AM_CLASSPATH,
    AM_DEBUG_LAUNCH,
    // Do not include AM_REST_KEY: it is supposed to be secret.

    // drill.yarn.zk

    ZK_CONNECT,
    ZK_ROOT,
    ZK_RETRY_COUNT,
    ZK_RETRY_DELAY_MS,
    ZK_FAILURE_TIMEOUT_MS,

    // drill.yarn.drillbit

    DRILLBIT_MEMORY,
    DRILLBIT_VCORES,
    DRILLBIT_DISKS,
    DRILLBIT_VM_ARGS,
    DRILLBIT_HEAP,
    DRILLBIT_DIRECT_MEM,
    DRILLBIT_PREFIX_CLASSPATH,
    DRILLBIT_EXTN_CLASSPATH,
    DRILLBIT_CLASSPATH,
    DRILLBIT_MAX_RETRIES,
    DRILLBIT_DEBUG_LAUNCH,
    DRILLBIT_HTTP_PORT,
    DISABLE_YARN_LOGS,

    // drill.yarn.http

    HTTP_ENABLED,
    HTTP_PORT
  };

  private static String envVars[] = {
      APP_ID_ENV_VAR,
//      RM_TRACKING_ENV_VAR,
      DRILL_HOME_ENV_VAR,
      DRILL_SITE_ENV_VAR,
      AM_HEAP_ENV_VAR,
      AM_JAVA_OPTS_ENV_VAR,
      DRILL_CLASSPATH_PREFIX_ENV_VAR,
      DRILL_CLASSPATH_ENV_VAR,
      DRILL_ARCHIVE_ENV_VAR,
      SITE_ARCHIVE_ENV_VAR,
//      SITE_DIR_ENV_VAR,
//      SITE_DIR_NAME_ENV_VAR,
      DRILL_DEBUG_ENV_VAR
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

  public void dumpEnv( PrintStream out )
  {
    out.print( "environment" );
    out.println( "[" );
    for ( String envVar : envVars ) {
      String value = System.getenv( envVar );
      out.print( envVar );
      out.print( " = " );
      if ( value == null ) {
        out.print( "<unset>" );
      }
      else {
        out.print( "\"" );
        out.print( value );
        out.print( "\"" );
      }
      out.println( );
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

    // Add environment variables as "pseudo" properties,
    // prefixed with "envt.".

    for ( String envVar : envVars ) {
      pairs.add( new NameValuePair( "envt." + envVar,
                 System.getenv( envVar ) ) );
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
   * Get the location of Drill home on a remote machine, relative to
   * the container working directory. Used when
   * constructing a launch context. Assumes either the absolute path
   * from the config file, or a constructed path to the localized
   * Drill on the remote node. YARN examples use "./foo" to refer to
   * container resources. But, since we cannot be sure when such a path
   * is evaluated, we explicitly use YARN's
   * PWD environment variable to get the absolute path.
   *
   * @return the remote path, with the "$PWD" environment variable.
   * @throws DoyConfigException
   */

  public String getRemoteDrillHome( ) throws DoyConfigException
  {
    // If the application is not localized, then the user can tell us the remote
    // path in the config file. Otherwise, we assume that the remote path is the
    // same as the local path.

    if ( ! config.getBoolean( LOCALIZE_DRILL ) ) {
      String drillHomePath = config.getString( DRILL_HOME );
      if ( DoYUtil.isBlank( drillHomePath ) ) {
        drillHomePath =  drillHome.getAbsolutePath();
      }
      return drillHomePath;
    }

    // The application is localized. Work out the location within the container
    // directory. The path starts with the "key" we specify when uploading the
    // Drill archive; YARN expands the archive into a folder of that name.

    String drillHome = "$PWD/" + config.getString( DRILL_ARCHIVE_KEY );

    String home = config.getString( DRILL_DIR_NAME );
    if ( DoYUtil.isBlank( home ) ) {
      // Assume the archive expands without a subdirectory.
    }

    // If the special "<base>" marker is used, assume that the path depends
    // on the name of the archive, which we know from the config file.

    else if ( home.equals( BASE_NAME_MARKER ) ) {

      // Otherwise, assume that the archive expands to a directory with the
      // same name as the archive itself (minus the archive suffix.)

      String drillArchivePath = config.getString( DrillOnYarnConfig.DRILL_ARCHIVE_PATH );
      if ( DoYUtil.isBlank( drillArchivePath ) ) {
        throw new DoyConfigException( "Required config property not set: " + DrillOnYarnConfig.DRILL_ARCHIVE_PATH );
      }
      File localArchiveFile = new File( drillArchivePath );
      home = localArchiveFile.getName( );
      String suffix = findSuffix( home );
      if ( suffix == null ) {
        throw new DoyConfigException( DrillOnYarnConfig.DRILL_ARCHIVE_PATH + " does not name a valid archive: " + drillArchivePath );
      }
      drillHome +=  "/" + home.substring( 0, home.length() - suffix.length() );
    }
    else {
      // If the user told us the name of the directory within the archive,
      // use it.

      drillHome += "/" + home;
    }
    return drillHome;
  }

  /**
   * Get the optional remote site directory name. This name will
   * include the absolute path for a non-localized application. It
   * will return the path relative to the container for a localized
   * application. In the localized case, the site archive is tar'ed
   * relative to the site directory so that its contents are unarchived
   * directly into the YARN-provided folder (with the name of the archive)
   * key. That is, if the site directory on the client is /var/drill/my-site,
   * the contents of the tar file will be "./drill-override.conf", etc.,
   * and the remote location is $PWD/site-key/drill-override.conf, where
   * site-key is the key name used to localize the site archive.
   *
   * @return
   */

  public String getRemoteSiteDir( )
  {
    // If the application does not use a site directory, then return null.

    if ( ! hasSiteDir( ) ) {
      return null;
    }

    // If the application is not localized, then use the remote site path
    // provided in the config file. Otherwise, assume that the remote path
    // is the same as the local path.

    if ( ! config.getBoolean( LOCALIZE_DRILL ) ) {
      String drillSitePath = config.getString( SITE_DIR );
      if ( DoYUtil.isBlank( drillSitePath ) ) {
        drillSitePath =  drillSite.getAbsolutePath();
      }
      return drillSitePath;
    }

    // Work out the site directory name as above for the Drill directory.
    // The caller must include a archive subdirectory name if required.

    return "$PWD/" + config.getString( SITE_ARCHIVE_KEY );
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

  /**
   * Return the app ID file to use for this client run. The file is
   * in the directory that holds the site dir (if a site dir is used),
   * else the directory that holds Drill home (otherwise.) Not that
   * the file does NOT go into the site dir or Drill home as we upload
   * these directories (via archives) to DFS so we don't want to change
   * them by adding a file.
   *
   * @param clusterId
   * @return
   */

  public File getLocalAppIdFile( String clusterId ) {
    File appIdDir;
    if ( hasSiteDir( ) ) {
      appIdDir = drillSite.getParentFile();
    } else {
      appIdDir = drillHome.getParentFile();
    }
    String appIdFileName = clusterId + ".appid";
    return new File( appIdDir, appIdFileName );
  }

  public boolean hasSiteDir( ) {
    return drillSite != null;
  }

  public File getLocalSiteDir( ) {
    return drillSite;
  }

  /**
   * Returns the DFS path to the localized Drill archive. This
   * is an AM-only method as it relies on an environment variable
   * set by the client. It is set only if the application is localized,
   * it is not set for a non-localized run.
   *
   * @return
   */

  public String getDrillArchiveDfsPath() {
    return System.getenv( DrillOnYarnConfig.DRILL_ARCHIVE_ENV_VAR );
  }

  /**
   * Returns the DFS path to the localized site archive. This
   * is an AM-only method as it relies on an environment variable
   * set by the client. This variable is optional; if not set
   * then the AM can infer that the application does not use a
   * site archive (configuration files reside in $DRILL_HOME/conf),
   * or the application is not localized.
   *
   * @return
   */

  public String getSiteArchiveDfsPath() {
    return System.getenv( DrillOnYarnConfig.SITE_ARCHIVE_ENV_VAR );
  }
}
