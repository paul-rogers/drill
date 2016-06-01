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
package org.apache.drill.yarn;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests to test the many ways that the Drill shell scripts can run.
 * Since it would be difficult to test options using the actual Drillbit, the
 * scripts make use of a special test fixture in runbit: the ability to pass
 * a "wrapper" script to run in place of the Drillit. That script probes stderr,
 * stdout and log files, and writes its arguments (which is the Drillbit launch
 * command) to a file. As a result, we can capture this output and analyze it
 * to ensure we are passing the right arguments to the Drillbit, and that output
 * is going to the right destinations.
 */

public class TestScripts
{
  public static final String MASTER_DISTRIB = "/Users/progers/git/drill/distribution/target/apache-drill-1.7.0-SNAPSHOT/apache-drill-1.7.0-SNAPSHOT";
  public static final String MASTER_SOURCE = "/Users/progers/git/drill";
  public static final String JAVA_HOME = "/Library/Java/JavaVirtualMachines/jdk1.7.0_79.jdk/Contents/Home";
  public static final boolean USE_SOURCE = true;
  public static final String TEMP_DIR = "/tmp";

  public static boolean useSource = USE_SOURCE;
  public static String javaHome = JAVA_HOME;
  public static File testDir;
  public static File testDistribDir;
  public static File testSiteDir;
  private static File testLogDir;

  /**
   * Out-of-the-box command-line arguments when launching Drill.
   * Order is not important here (though it is to Java.)
   */

  public static String stdArgs[] = {
      "-Xms4G",
      "-Xmx4G",
      "-XX:MaxDirectMemorySize=8G",
      "-XX:MaxPermSize=512M",
      "-XX:ReservedCodeCacheSize=1G",
      "-Ddrill.exec.enable-epoll=true",
      "-XX:+CMSClassUnloadingEnabled",
      "-XX:+UseG1GC",
      "-Dlog.path=/private/tmp/script-test/drill/log/drillbit.log",
      "-Dlog.query.path=/private/tmp/script-test/drill/log/drillbit_queries.json",
      "-cp",
      "org.apache.drill.exec.server.Drillbit"
  };

  /**
   * Out-of-the-box class-path before any custom additions.
   */

  private static String stdCp[] =
  {
    "conf",
    "jars/*",
    "jars/ext/*",
    "jars/3rdparty/*",
    "jars/classb/*"
  };

  /**
   * Directories to create to simulate a Drill distribution.
   */

  private static String distribDirs[] = {
      "bin",
      "jars",
      "jars/3rdparty",
      "jars/ext",
      "conf"
  };

  /**
   * Out-of-the-box Jar directories.
   */

  private static String jarDirs[] = {
      "jars",
      "jars/3rdparty",
      "jars/ext",
  };

  /**
   * Scripts we must copy from the source tree to create a simulated
   * Drill bin directory.
   */

  public static String scripts[] = {
      "drill-config.sh",
      //drill-embedded
      //drill-localhost
      "drill-on-yarn.sh",
      "drillbit.sh",
      //dumpcat
      //hadoop-excludes.txt
      "runbit",
      "sqlline",
      "start-bit.sh",
      //sqlline.bat
      //submit_plan
      "yarn-drillbit.sh"
  };

  /**
   * Create the basic test directory. Tests add or remove details.
   */

  @BeforeClass
  public static void initialSetup( ) throws IOException {
    File tempDir = new File( TEMP_DIR );
    testDir = new File( tempDir, "script-test" );
    testDistribDir = new File( testDir, "drill" );
    testSiteDir = new File( testDir, "site" );
    testLogDir = new File( testDir, "logs" );
    if ( testDir.exists() ) {
      FileUtils.forceDelete( testDir );
    }
    testDir.mkdirs();
    testSiteDir.mkdir();
    testLogDir.mkdir( );
  }

  public void createMockDistrib( ) throws IOException
  {
    if ( useSource ) {
      buildFromSource( );
    }
    else {
      buildFromDistrib( );
    }
  }

  /**
   * Build the Drill distribution directory directly from sources.
   */

  private void buildFromSource() throws IOException {
    createMockDirs( );
    File drillDir = new File( MASTER_SOURCE );
    File resources = new File( drillDir, "distribution/src/resources" );
    copyScripts( resources );
  }

  /**
   * Build the shell of a Drill distribution directory by creating the
   * required directory structure.
   */

  private void createMockDirs() throws IOException {
    if ( testDistribDir.exists() ) {
      FileUtils.forceDelete( testDistribDir );
    }
    testDistribDir.mkdir();
    for ( String path : distribDirs ) {
      File subDir = new File( testDistribDir, path );
      subDir.mkdirs();
    }
    for ( String path : jarDirs ) {
      makeDummyJar( new File( testDistribDir, path ), "dist" );
    }
  }

  /**
   * The tests should not require jar files, but we simulate them to be a bit
   * more realistic. Since we dont' run Java, the jar files can be simulated.
   */

  private File makeDummyJar(File dir, String prefix) throws IOException {
    String jarName = "";
    if ( prefix != null ) {
      jarName += prefix + "-";
    }
    jarName += dir.getName( ) + ".jar";
    File jarFile = new File( dir, jarName );
    writeFile( jarFile, "Dummy jar" );
    return jarFile;
  }

  /**
   * Create a simple text file with the given contents.
   */

  public void writeFile( File file, String contents ) throws IOException
  {
    PrintWriter out = new PrintWriter( new FileWriter( file ) );
    out.println( contents );
    out.close( );
  }

  /**
   * Create a drill-env.sh or distrib-env.sh file with the given environment in
   * the recommended format.
   */

  private void createEnvFile(File file, Map<String, String> env) throws IOException {
    PrintWriter out = new PrintWriter( new FileWriter( file ) );
    out.println( "#!/usr/bin/env bash" );
    for ( String key : env.keySet() ) {
      String value = env.get( key );
      out.print( "export " );
      out.print( key );
      out.print( "=${" );
      out.print( key );
      out.print(":-\"" );
      out.print( value );
      out.println( "\"}" );
    }
    out.close( );
  }

  /**
   * Copy the standard scripts from source location to the mock distribution
   * directory.
   */

  private void copyScripts(File sourceDir) throws IOException {
    File binDir = new File( testDistribDir, "bin" );
    for ( String script : scripts ) {
      File source = new File( sourceDir, script );
      File dest = new File( binDir, script );
      Files.copy(source.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
      dest.setExecutable( true );
    }

    // Create the "magic" wrapper script that simulates the Drillbit and
    // captures the output we need for testing.

    String wrapper = "wrapper.sh";
    File dest = new File( binDir, wrapper );
    InputStream is = getClass( ).getResourceAsStream( wrapper );
    Files.copy( is, dest.toPath(), StandardCopyOption.REPLACE_EXISTING );
    is.close();
    dest.setExecutable( true );
  }

  private void buildFromDistrib() {
    // TODO Auto-generated method stub

  }

  /**
   * Consume the input from a stream, specifically the stderr or
   * stdout stream from a process.
   *
   * @see http://stackoverflow.com/questions/14165517/processbuilder-forwarding-stdout-and-stderr-of-started-processes-without-blocki
   */

  private static class StreamGobbler extends Thread
  {
    InputStream is;
    public StringBuilder buf = new StringBuilder( );

    private StreamGobbler(InputStream is) {
      this.is = is;
    }

    @Override
    public void run() {
      try {
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);
        String line = null;
        while ((line = br.readLine()) != null) {
          buf.append( line );
          buf.append( "\n" );
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }
  }

  /**
   * Handy run result class to capture the information we need for testing and to do
   * various kinds of validation on it.
   */

  public static class RunResult
  {
    File logDir;
    File logFile;
    String stdout;
    String stderr;
    List<String> echoArgs;
    int returnCode;
    String classPath[];
    private String log;

    /**
     * Split the class path into strings for easier validation.
     */

    public void analyze() {
      if ( echoArgs == null ) {
        return;
      }
      for ( int i = 0;  i < echoArgs.size( );  i++ ) {
        String arg = echoArgs.get( i );
        if ( arg.equals( "-cp" ) ) {
          classPath = Pattern.compile( ":" ).split( ( echoArgs.get( i + 1 ) ) );
          break;
        }
      }
    }

    /**
     * Read the log file, if any, generated by the process.
     */

    public void loadLog( ) throws IOException {
      StringBuilder buf = new StringBuilder( );
      BufferedReader reader;
      try {
        reader = new BufferedReader( new FileReader( logFile ) );
      } catch (FileNotFoundException e) {
        return;
      }
      String line;
      while ( (line = reader.readLine()) != null ) {
        buf.append( line );
        buf.append( "\n" );
      }
      reader.close();
      log = buf.toString( );
    }

    /**
     * Validate that the first argument invokes Java correctly.
     */

    public void validateJava( ) {
      assertNotNull( echoArgs );
      String java = javaHome + "/bin/java";
      List<String> actual = echoArgs;
      assertEquals( java, actual.get( 0 ) );
    }

    public boolean containsArg( String arg ) {
      for ( String actual: echoArgs ) {
        if ( actual.equals( arg ) ) {
          return true; }
      }
      return false;
    }

    public void validateArgs( ) {
      validateArgs( stdArgs );
    }

    public void validateArg( String arg ) {
      validateArgs( Collections.singletonList( arg ) );
    }

    public void validateArgs( String args[] ) {
      validateArgs( Arrays.asList( args ) );
    }

    public void validateArgs( List<String> args) {
      validateJava( );
      for ( String arg : args ) {
        assertTrue( containsArg( arg ) );
      }
    }

    public void validateArgRegex( String arg ) {
      assertTrue( containsArgRegex( arg ) );
    }

    public void validateArgsRegex( List<String> args) {
      assertTrue( containsArgsRegex( args ) );
    }

    public boolean containsArgsRegex( List<String> args) {
      for ( String arg : args ) {
        if ( ! containsArgRegex( arg ) ) {
          return false; }
      }
      return true;
    }

    public boolean containsArgRegex( String arg ) {
      for ( String actual: echoArgs ) {
        if ( actual.matches( arg ) ) {
          return true; }
      }
      return false;
    }

    public void validateClassPath( String expectedCP) {
      assertTrue( classPathContains( expectedCP ) );
    }

    public void validateClassPath( String expectedCP[]) {
      assertTrue( classPathContains( expectedCP ) );
    }

    public boolean classPathContains( String expectedCP[]) {
      for ( String entry : expectedCP ) {
        if ( ! classPathContains( entry ) ) {
          return false;
        }
      }
      return true;
    }

    public boolean classPathContains( String expectedCP) {
      if ( classPath == null ) {
        fail( "No classpath returned" );
      }
      String tail = "/" + testDir.getName( ) + "/" + testDistribDir.getName() + "/";
      String expectedPath;
      if ( expectedCP.startsWith("/") ) {
        expectedPath = expectedCP;
      } else {
        expectedPath = tail + expectedCP;
      }
      for ( String entry : classPath ) {
        if ( entry.endsWith( expectedPath ) ) {
          return true;
        }
      }
      return false;
    }

  }

  private void cleanLogs(File logDir) throws IOException {
    if ( logDir.exists( ) ) {
      FileUtils.forceDelete( logDir );
    }
  }

  /**
   * The "business end" of the tests: runs drillbit.sh and captures results.
   */

  private RunResult runDrillbit( String args[ ], Map<String,String> env ) throws IOException {
    File outputFile = new File( testDir, "output.txt" );
    outputFile.delete();
    List<String> cmd = new ArrayList<>( );
    String drillbit = testDistribDir.getName() + "/bin/drillbit.sh";
    cmd.add( drillbit );
    for ( String arg : args ) {
      cmd.add( arg );
    }
    ProcessBuilder pb = new ProcessBuilder( )
        .command(cmd)
        .directory( testDir );
    Map<String,String> pbEnv = pb.environment();
    pbEnv.clear();
    if ( env != null ) {
      pbEnv.putAll( env );
    }
    File binDir = new File( testDistribDir, "bin" );
    File wrapperCmd = new File( binDir, "wrapper.sh" );

    // Set the magic wrapper to capture output.

    pbEnv.put( "_DRILL_WRAPPER_", wrapperCmd.getAbsolutePath() );
    pbEnv.put( "JAVA_HOME", javaHome );
    Process proc = pb.start( );
    StreamGobbler errorGobbler = new StreamGobbler(proc.getErrorStream());
    StreamGobbler outputGobbler = new StreamGobbler(proc.getInputStream());
    outputGobbler.start();
    errorGobbler.start();

    try {
      proc.waitFor();
    } catch (InterruptedException e) {
      // Won't occur.
    }

    RunResult result = new RunResult( );
    result.stderr = errorGobbler.buf.toString();
    result.stdout = outputGobbler.buf.toString();
    result.returnCode = proc.exitValue();
    if ( result.returnCode != 0 ) {
      return result;
    }

    // Capture the Java arguments which the wrapper script wrote to a file.

    BufferedReader reader;
    try {
      reader = new BufferedReader( new FileReader( outputFile ) );
    } catch (FileNotFoundException e) {
      return result;
    }
    try {
      result.echoArgs = new ArrayList<>( );
      String line;
      while ( (line = reader.readLine()) != null ) {
        result.echoArgs.add( line );
      }
      result.analyze( );
    }
    finally {
      reader.close( );
    }
    return result;
  }

  /**
   * Run drillbit.sh and capture log output as well from the given log directory.
   */

  private RunResult runDrillbit( String args[ ], Map<String,String> env, File logDir ) throws IOException {
    if ( logDir == null ) {
      logDir = new File( testDistribDir, "log" );
    }
    cleanLogs( logDir );
    RunResult result = runDrillbit( args, env );
    result.logDir = logDir;
    result.logFile = new File( logDir, "drillbit.log" );
    if ( result.logFile.exists() ) {
      result.loadLog( );
    }
    else {
      result.logFile = null;
    }
    return result;
  }

  /**
   * Build a "starter" conf or site directory by creating a mock drill-override.conf
   * file.
   */

  private void createMockConf(File siteDir) throws IOException {
    createDir( siteDir );
    File override = new File( siteDir, "drill-override.conf" );
    writeFile( override, "# Dummy override" );
  }

  /**
   * Ensure that the Drill log file contains at least the sample message
   * written by the wrapper.
   */

  private void validateDrillLog(RunResult result) {
    assertNotNull( result.log );
    assertTrue( result.log.contains( "Drill Log Message" ) );
  }

  /**
   * Validate that the stdout contained the expected message.
   */

  private void validateStdOut(RunResult result) {
    assertTrue( result.stdout.contains( "Starting drillbit on" ) );
  }

  /**
   * Validate that the stderr contained the sample error message from
   * the wrapper.
   */

  private void validateStdErr(RunResult result) {
    assertTrue( result.stderr.contains( "Stderr Message" ) );
  }

  private void removeDir(File dir) throws IOException {
    if ( dir.exists() ) {
      FileUtils.forceDelete( dir );
    }
  }

  /**
   * Remove, then create a directory.
   */

  private File createDir(File dir) throws IOException {
    removeDir( dir );
    dir.mkdirs();
    return dir;
  }

  /**
   * Test the simplest case: use the $DRILL_HOME/conf directory and default
   * log location. Non-existent drill-env.sh and drill-config.sh files.
   * Everything is at its Drill-provided defaults. Then, try overriding each
   * user-settable environment variable in the environment (which simulates
   * what YARN might do.)
   */

  @Test
  public void testStockCombined( ) throws IOException
  {
    createMockDistrib( );
    File siteDir = new File( testDistribDir, "conf" );
    createMockConf( siteDir );

    // No drill-env.sh, no distrib-env.sh

    {
      RunResult result = runDrillbit( new String[ ] { "run" }, null, null );
      assertEquals( 0, result.returnCode );
      result.validateArgs( );
      result.validateClassPath( stdCp );
      validateStdOut( result );
      validateStdErr( result );
      validateDrillLog( result );
    }

    // As above, but pass an argument.

    {
      String propArg = "-Dproperty=value";
      RunResult result = runDrillbit( new String[ ] { "run", propArg }, null, null );
      assertEquals( 0, result.returnCode );
      validateStdOut( result );
      result.validateArg( propArg );
    }

    // Custom Java Opts to achieve the same result

    {
      String propArg = "-Dproperty=value";
      Map<String,String> env = new HashMap<>( );
      env.put( "DRILL_JAVA_OPTS", propArg );
      RunResult result = runDrillbit( new String[ ] { "run" }, env, null );
      assertEquals( 0, result.returnCode );
      result.validateArgs( ); // Should not lose standard JVM args
      validateStdOut( result );
      result.validateArg( propArg );
    }

    // Custom Drillbit Java Opts to achieve the same result

    {
      String propArg = "-Dproperty2=value2";
      Map<String,String> env = new HashMap<>( );
      env.put( "DRILLBIT_JAVA_OPTS", propArg );
      RunResult result = runDrillbit( new String[ ] { "run" }, env, null );
      assertEquals( 0, result.returnCode );
      result.validateArgs( ); // Should not lose standard JVM args
      validateStdOut( result );
      result.validateArg( propArg );
    }

    // Both sets of options

    {
      String propArg = "-Dproperty=value";
      String propArg2 = "-Dproperty2=value2";
      Map<String,String> env = new HashMap<>( );
      env.put( "DRILL_JAVA_OPTS", propArg );
      env.put( "DRILLBIT_JAVA_OPTS", propArg2 );
      RunResult result = runDrillbit( new String[ ] { "run" }, env, null );
      assertEquals( 0, result.returnCode );
      result.validateArgs( new String[] { propArg, propArg2 } );
    }

    // Custom heap memory

    {
      Map<String,String> env = new HashMap<>( );
      env.put( "DRILL_HEAP", "5G" );
      RunResult result = runDrillbit( new String[ ] { "run" }, env, null );
      String args[] = { "-Xms5G", "-Xmx5G" };
      result.validateArgs( args );
    }

    // Custom direct memory

    {
      Map<String,String> env = new HashMap<>( );
      env.put( "DRILL_MAX_DIRECT_MEMORY", "7G" );
      RunResult result = runDrillbit( new String[ ] { "run" }, env, null );
      result.validateArg( "-XX:MaxDirectMemorySize=7G" );
    }

    // Enable GC logging

    {
      Map<String,String> env = new HashMap<>( );
      env.put( "SERVER_LOG_GC", "1" );
      RunResult result = runDrillbit( new String[ ] { "run" }, env, null );
      String logTail = testDistribDir.getName() + "/log/drillbit.gc";
      result.validateArgRegex( "-Xloggc:.*/" + logTail );
    }

    // Max Perm Size

    {
      Map<String,String> env = new HashMap<>( );
      env.put( "DRILLBIT_MAX_PERM", "600M" );
      RunResult result = runDrillbit( new String[ ] { "run" }, env, null );
      result.validateArg( "-XX:MaxPermSize=600M" );
    }

    // Code cache size

    {
      Map<String,String> env = new HashMap<>( );
      env.put( "DRILLBIT_CODE_CACHE_SIZE", "2G" );
      RunResult result = runDrillbit( new String[ ] { "run" }, env, null );
      result.validateArg( "-XX:ReservedCodeCacheSize=2G" );
    }
  }

  /**
   * Use the "stock" setup, but add each custom bit of the class path to ensure
   * it is passed to the Drillbit.
   *
   * @throws IOException
   */

  @Test
  public void testClassPath( ) throws IOException
  {
    createMockDistrib( );
    File siteDir = new File( testDistribDir, "conf" );
    createMockConf( siteDir );

    File extrasDir = createDir( new File( testDir, "extras" ) );
    File hadoopJar = makeDummyJar( extrasDir, "hadoop" );
    File hbaseJar = makeDummyJar( extrasDir, "hbase" );
    File prefixJar = makeDummyJar( extrasDir, "prefix" );
    File cpJar = makeDummyJar( extrasDir, "cp" );
    File extnJar = makeDummyJar( extrasDir, "extn" );
    File toolsJar = makeDummyJar( extrasDir, "tools" );

    {
      Map<String,String> env = new HashMap<>( );
      env.put( "DRILL_CLASSPATH_PREFIX", prefixJar.getAbsolutePath() );
      RunResult result = runDrillbit( new String[ ] { "run" }, env, null );
      result.validateClassPath( prefixJar.getAbsolutePath() );
    }

    {
      Map<String,String> env = new HashMap<>( );
      env.put( "DRILL_TOOL_CP", toolsJar.getAbsolutePath() );
      RunResult result = runDrillbit( new String[ ] { "run" }, env, null );
      result.validateClassPath( toolsJar.getAbsolutePath() );
    }

    {
      Map<String,String> env = new HashMap<>( );
      env.put( "HADOOP_CLASSPATH", hadoopJar.getAbsolutePath() );
      RunResult result = runDrillbit( new String[ ] { "run" }, env, null );
      result.validateClassPath( hadoopJar.getAbsolutePath() );
    }

    {
      Map<String,String> env = new HashMap<>( );
      env.put( "HBASE_CLASSPATH", hbaseJar.getAbsolutePath() );
      RunResult result = runDrillbit( new String[ ] { "run" }, env, null );
      result.validateClassPath( hbaseJar.getAbsolutePath() );
    }

    {
      Map<String,String> env = new HashMap<>( );
      env.put( "EXTN_CLASSPATH", extnJar.getAbsolutePath() );
      RunResult result = runDrillbit( new String[ ] { "run" }, env, null );
      result.validateClassPath( extnJar.getAbsolutePath() );
    }

    {
      Map<String,String> env = new HashMap<>( );
      env.put( "DRILL_CLASSPATH", cpJar.getAbsolutePath() );
      RunResult result = runDrillbit( new String[ ] { "run" }, env, null );
      result.validateClassPath( cpJar.getAbsolutePath() );
    }

    // Site jars not on path if not created

    File siteJars = new File( siteDir, "jars" );
    {
      RunResult result = runDrillbit( new String[ ] { "run" }, null, null );
      assertFalse( result.classPathContains( siteJars.getAbsolutePath() ) );
    }

    // Site/jars on path if exists

    createDir( siteJars );
    makeDummyJar( siteJars, "site" );

    {
      RunResult result = runDrillbit( new String[ ] { "run" }, null, null );
      result.validateClassPath( siteJars.getAbsolutePath() + "/*" );
    }
  }

  /**
   * Create a custom log folder location.
   */

  @Test
  public void testLogDir( ) throws IOException
  {
    createMockDistrib( );
    File siteDir = new File( testDistribDir, "conf" );
    createMockConf( siteDir );
    File logsDir = createDir( new File( testDir, "logs" ) );
    removeDir( new File( testDistribDir, "log" ) );

    {
      String logPath = logsDir.getAbsolutePath();
      Map<String,String> env = new HashMap<>( );
      env.put( "DRILL_LOG_DIR", logPath );
      RunResult result = runDrillbit( new String[ ] { "run" }, env, logsDir );
      assertEquals( 0, result.returnCode );
      result.validateArgs( new String[] {
          "-Dlog.path=" + logPath + "/drillbit.log",
          "-Dlog.query.path=" + logPath + "/drillbit_queries.json",
      } );
      validateStdOut( result );
      validateStdErr( result );
      validateDrillLog( result );
    }

  }

  /**
   * Try setting custom environment variable values in drill-env.sh in
   * the $DRILL_HOME/conf location.
   */

  @Test
  public void testDrillEnv( ) throws IOException
  {
    doEnvFileTest( "drill-env.sh" );
  }

  /**
   * Repeat the above test using distrib-env.sh in
   * the $DRILL_HOME/conf location.
   */

  @Test
  public void testDistribEnv( ) throws IOException
  {
    doEnvFileTest( "distrib-env.sh" );
  }

  /**
   * Implementation of the drill-env.sh and distrib-env.sh tests.
   */

  private void doEnvFileTest( String fileName ) throws IOException
  {
    createMockDistrib( );
    File siteDir = new File( testDistribDir, "conf" );
    createMockConf( siteDir );

    /**
     * Set all properties in the env file.
     */

    Map<String,String> drillEnv = new HashMap<>( );
    String propArg = "-Dproperty=value";
    drillEnv.put( "DRILL_JAVA_OPTS", propArg );
    drillEnv.put( "DRILL_HEAP", "5G" );
    drillEnv.put( "DRILL_MAX_DIRECT_MEMORY", "7G" );
    drillEnv.put( "SERVER_LOG_GC", "1" );
    drillEnv.put( "DRILLBIT_MAX_PERM", "600M" );
    drillEnv.put( "DRILLBIT_CODE_CACHE_SIZE", "2G" );
    createEnvFile( new File( siteDir, fileName ), drillEnv );

    {
      RunResult result = runDrillbit( new String[ ] { "run" }, null, null );
      assertEquals( 0, result.returnCode );

      String expectedArgs[] = {
          propArg,
          "-Xms5G", "-Xmx5G",
          "-XX:MaxDirectMemorySize=7G",
          "-XX:ReservedCodeCacheSize=2G",
          "-XX:MaxPermSize=600M"
      };

      result.validateArgs( expectedArgs );
      String logTail = testDistribDir.getName() + "/log/drillbit.gc";
      result.validateArgRegex( "-Xloggc:.*/" + logTail );
    }

    // Change some drill-env.sh options in the environment.
    // The generated drill-env.sh should allow overrides.
    // (The generated form is the form that customers should use.)

    {
      Map<String,String> env = new HashMap<>( );
      env.put( "SERVER_LOG_GC", "0" );
      env.put( "DRILL_MAX_DIRECT_MEMORY", "9G" );

      RunResult result = runDrillbit( new String[ ] { "run" }, env, null );
      assertEquals( 0, result.returnCode );
      result.validateArg( "-XX:MaxDirectMemorySize=9G" );
      result.validateArg( "-XX:MaxPermSize=600M" );
      String logTail = testDistribDir.getName() + "/log/drillbit.gc";
      assertFalse( result.containsArgRegex( "-Xloggc:.*/" + logTail ) );
    }
  }

  /**
   * Test that drill-env.sh overrides distrib-env.sh, and that the
   * environment overrides both. Assumes the basics were tested above.
   *
   * @throws IOException
   */

  @Test
  public void testDrillAndDistribEnv( ) throws IOException
  {
    createMockDistrib( );
    File siteDir = new File( testDistribDir, "conf" );
    createMockConf( siteDir );

    Map<String,String> distribEnv = new HashMap<>( );
    distribEnv.put( "DRILL_HEAP", "5G" );
    distribEnv.put( "DRILL_MAX_DIRECT_MEMORY", "7G" );
    distribEnv.put( "DRILLBIT_MAX_PERM", "600M" );
    createEnvFile( new File( siteDir, "distrib-env.sh" ), distribEnv );

    Map<String,String> drillEnv = new HashMap<>( );
    drillEnv.put( "DRILL_HEAP", "6G" );
    drillEnv.put( "DRILL_MAX_DIRECT_MEMORY", "9G" );
    createEnvFile( new File( siteDir, "drill-env.sh" ), drillEnv );

    {
      RunResult result = runDrillbit( new String[ ] { "run" }, null, null );
      assertEquals( 0, result.returnCode );
      String expectedArgs[] = {
          "-Xms6G", "-Xmx6G",
          "-XX:MaxDirectMemorySize=9G",
          "-XX:MaxPermSize=600M",
          "-XX:ReservedCodeCacheSize=1G" // Default
      };

      result.validateArgs(expectedArgs );
    }

    {
      Map<String,String> env = new HashMap<>( );
      env.put( "DRILL_MAX_DIRECT_MEMORY", "5G" );

      RunResult result = runDrillbit( new String[ ] { "run" }, env, null );
      assertEquals( 0, result.returnCode );
      String expectedArgs[] = {
          "-Xms6G", "-Xmx6G",
          "-XX:MaxDirectMemorySize=5G",
          "-XX:MaxPermSize=600M",
          "-XX:ReservedCodeCacheSize=1G" // Default
      };

      result.validateArgs( expectedArgs );
    }
  }

  @Test
  public void testBadSiteDir( ) throws IOException
  {
  }

  /**
   * Move configuration to a site folder out of $DRILL_HOME/conf.
   * The site folder can contain code (which is why we call it "site" and
   * not "config".) The site directory can be passed to the Drillbit
   * in several ways.
   *
   * @throws IOException
   */

  @Test
  public void testSiteDir( ) throws IOException
  {
    createMockDistrib( );
    File confDir = new File( testDistribDir, "conf" );
    createDir( confDir );
    File siteDir = new File( testDir, "site" );
    createMockConf( siteDir );

    // Dummy drill-env.sh to simulate the shipped "example" file.

    writeFile( new File( confDir, "drill-env.sh" ),
        "#!/usr/bin/env bash\n" +
        "# Example file"
        );
    File siteJars = new File( siteDir, "jars" );

    Map<String,String> distribEnv = new HashMap<>( );
    distribEnv.put( "DRILL_HEAP", "5G" );
    distribEnv.put( "DRILL_MAX_DIRECT_MEMORY", "7G" );
    distribEnv.put( "DRILLBIT_MAX_PERM", "600M" );
    createEnvFile( new File( confDir, "distrib-env.sh" ), distribEnv );

    Map<String,String> drillEnv = new HashMap<>( );
    drillEnv.put( "DRILL_HEAP", "6G" );
    drillEnv.put( "DRILL_MAX_DIRECT_MEMORY", "9G" );
    createEnvFile( new File( siteDir, "drill-env.sh" ), drillEnv );

    String expectedArgs[] = {
        "-Xms6G", "-Xmx6G",
        "-XX:MaxDirectMemorySize=9G",
        "-XX:MaxPermSize=600M",
        "-XX:ReservedCodeCacheSize=1G" // Default
    };

    // Site set using argument

    {
      RunResult result = runDrillbit( new String[ ] { "run", "--config", siteDir.getAbsolutePath() }, null, null );
      assertEquals( 0, result.returnCode );
      result.validateArgs(expectedArgs );
      result.validateClassPath( siteDir.getAbsolutePath() );
    }

    {
      RunResult result = runDrillbit( new String[ ] { "--config", siteDir.getAbsolutePath(), "run" }, null, null );
      assertEquals( 0, result.returnCode );
      result.validateArgs(expectedArgs );
    }

    {
      RunResult result = runDrillbit( new String[ ] { "run", "--site", siteDir.getAbsolutePath() }, null, null );
      assertEquals( 0, result.returnCode );
      result.validateArgs(expectedArgs );
    }

    // Site argument and argument to Drillbit

    {
      String propArg = "-Dproperty=value";
      RunResult result = runDrillbit( new String[ ] { "run", "--site", siteDir.getAbsolutePath(), propArg }, null, null );
      assertEquals( 0, result.returnCode );
      result.validateArgs(expectedArgs );
      result.validateArg( propArg );
    }

    // Set as an environment variable

    {
      Map<String,String> env = new HashMap<>( );
      env.put( "DRILL_CONF_DIR", siteDir.getAbsolutePath() );
      RunResult result = runDrillbit( new String[ ] { "run" }, env, null );
      assertEquals( 0, result.returnCode );
      result.validateArgs(expectedArgs );
    }

    // Site jars not on path if not created

    {
      RunResult result = runDrillbit( new String[ ] { "run", "--site", siteDir.getAbsolutePath() }, null, null );
      assertFalse( result.classPathContains( siteJars.getAbsolutePath() ) );
    }

    // Site/jars on path if exists

    createDir( siteJars );
    makeDummyJar( siteJars, "site" );

    {
      RunResult result = runDrillbit( new String[ ] { "run", "--site", siteDir.getAbsolutePath() }, null, null );
      assertTrue( result.classPathContains( siteJars.getAbsolutePath() + "/*" ) );
    }
  }

  @Test
  public void testPidDir( ) throws IOException
  {
    createMockDistrib( );
    File siteDir = new File( testDistribDir, "conf" );
    createMockConf( siteDir );
    File pidDir = createDir( new File( testDir, "pid" ) );

    // TODO: Needs to create a PID file
  }
}
