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
  public static File testDrillHome;
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
      "-Ddrill\\.exec\\.enable-epoll=true",
      "-XX:\\+CMSClassUnloadingEnabled",
      "-XX:\\+UseG1GC",
      "-Dlog\\.path=/.*/script-test/drill/log/drillbit\\.log",
      "-Dlog\\.query\\.path=/.*/script-test/drill/log/drillbit_queries\\.json",
      "org\\.apache\\.drill\\.exec\\.server\\.Drillbit"
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
      "drill-embedded",
      "drill-localhost",
      "drill-on-yarn.sh",
      "drillbit.sh",
      "drill-conf",
      //dumpcat
      //hadoop-excludes.txt
      "runbit",
      "sqlline",
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
    testDrillHome = new File( testDir, "drill" );
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
    if ( testDrillHome.exists() ) {
      FileUtils.forceDelete( testDrillHome );
    }
    testDrillHome.mkdir();
    for ( String path : distribDirs ) {
      File subDir = new File( testDrillHome, path );
      subDir.mkdirs();
    }
    for ( String path : jarDirs ) {
      makeDummyJar( new File( testDrillHome, path ), "dist" );
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
    File binDir = new File( testDrillHome, "bin" );
    for ( String script : scripts ) {
      File source = new File( sourceDir, script );
      File dest = new File( binDir, script );
      copyFile(source, dest );
      dest.setExecutable( true );
    }

    // Create the "magic" wrapper script that simulates the Drillbit and
    // captures the output we need for testing.

    String wrapper = "wrapper.sh";
    File dest = new File( binDir, wrapper );
    InputStream is = getClass( ).getResourceAsStream( "/" + wrapper );
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
    public File pidFile;
    public File outFile;
    private String out;

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
      log = loadFile( logFile );
    }

    private String loadFile( File file ) throws IOException {
      StringBuilder buf = new StringBuilder( );
      BufferedReader reader;
      try {
        reader = new BufferedReader( new FileReader( file ) );
      } catch (FileNotFoundException e) {
        return null;
      }
      String line;
      while ( (line = reader.readLine()) != null ) {
        buf.append( line );
        buf.append( "\n" );
      }
      reader.close();
      return buf.toString( );
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

    public void validateStockArgs( ) {
      for ( String arg : stdArgs ) {
        assertTrue( "Argument not found: " + arg, containsArgRegex( arg ) );
      }
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

    public boolean containsArgsRegex( String args[]) {
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
      String tail = "/" + testDir.getName( ) + "/" + testDrillHome.getName() + "/";
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

    public void loadOut() throws IOException {
      out = loadFile( outFile );
    }

    /**
     * Ensure that the Drill log file contains at least the sample message
     * written by the wrapper.
     */

    public void validateDrillLog( ) {
      assertNotNull(  log );
      assertTrue(  log.contains( "Drill Log Message" ) );
    }

    /**
     * Validate that the stdout contained the expected message.
     */

    public void validateStdOut( ) {
      assertTrue(  stdout.contains( "Starting drillbit on" ) );
    }

    /**
     * Validate that the stderr contained the sample error message from
     * the wrapper.
     */

    public void validateStdErr( ) {
      assertTrue(  stderr.contains( "Stderr Message" ) );
    }

    public int getPid() throws IOException {
      BufferedReader reader = new BufferedReader( new FileReader( pidFile ) );
      int pid = Integer.parseInt( reader.readLine() );
      reader.close( );
      return pid;
    }

  }

  // Drillbit commands

  public static String DRILLBIT_RUN = "run";
  public static String DRILLBIT_START = "start";
  public static String DRILLBIT_STATUS = "status";
  public static String DRILLBIT_STOP = "stop";
  public static String DRILLBIT_RESTART = "restart";

  /**
   * The "business end" of the tests: runs drillbit.sh and captures results.
   */

  public static class ScriptRunner
  {
    public File cwd = testDir;
    public File drillHome = testDrillHome;
    public String script;
    public List<String> args = new ArrayList<>( );
    public Map<String,String> env = new HashMap<>( );
    public File logDir;
    public File pidFile;
    public File outputFile;
    public boolean preserveLogs;

    public ScriptRunner( String script ) {
      this.script = script;
    }

    public ScriptRunner( String script, String cmd ) {
      this( script );
      args.add( cmd );
    }

    public ScriptRunner( String script, String cmdArgs[] ) {
      this( script );
      for ( String arg : cmdArgs ) {
        args.add( arg );
      }
    }

    public ScriptRunner withArg( String arg ) {
      args.add( arg );
      return this;
    }

    public ScriptRunner withSite( File siteDir ) {
      if ( siteDir != null ) {
        args.add( "--site" );
        args.add( siteDir.getAbsolutePath() );
      }
      return this;
    }

    public ScriptRunner withEnvironment( Map<String,String> env ) {
      if ( env != null ) {
        this.env.putAll( env );
      }
      return this;
    }

    public ScriptRunner addEnv( String key, String value ) {
      env.put( key, value );
      return this;
    }

    public ScriptRunner withLogDir( File logDir ) {
      this.logDir = logDir;
      return this;
    }
    public ScriptRunner preserveLogs( ) {
      preserveLogs = true;
      return this;
    }

    public RunResult run( ) throws IOException {
      File binDir = new File( drillHome, "bin" );
      File scriptFile = new File( binDir, script );
      outputFile = new File( testDir, "output.txt" );
      outputFile.delete();
      if ( logDir == null ) {
        logDir = new File( testDrillHome, "log" );
      }
      if ( ! preserveLogs ) {
        cleanLogs( logDir );
      }

      Process proc = startProcess( scriptFile );
      RunResult result = runProcess( proc );
      if ( result.returnCode == 0 ) {
        captureOutput( result );
        captureLog( result );
      }
      return result;
    }

    private void cleanLogs(File logDir) throws IOException {
      if ( logDir.exists( ) ) {
        FileUtils.forceDelete( logDir );
      }
    }

    private Process startProcess( File scriptFile ) throws IOException {
      outputFile.delete();
      List<String> cmd = new ArrayList<>( );
      cmd.add( scriptFile.getAbsolutePath( ) );
      cmd.addAll( args );
      ProcessBuilder pb = new ProcessBuilder( )
          .command(cmd)
          .directory( cwd );
      Map<String,String> pbEnv = pb.environment();
      pbEnv.clear();
      pbEnv.putAll( env );
      File binDir = new File( drillHome, "bin" );
      File wrapperCmd = new File( binDir, "wrapper.sh" );

      // Set the magic wrapper to capture output.

      pbEnv.put( "_DRILL_WRAPPER_", wrapperCmd.getAbsolutePath() );
      pbEnv.put( "JAVA_HOME", javaHome );
      return pb.start( );
    }

    private RunResult runProcess(Process proc) {
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
      return result;
    }

    private void captureOutput(RunResult result) throws IOException {
      // Capture the Java arguments which the wrapper script wrote to a file.

      BufferedReader reader;
      try {
        reader = new BufferedReader( new FileReader( outputFile ) );
      } catch (FileNotFoundException e) {
        return;
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
    }

    private void captureLog(RunResult result) throws IOException {
      result.logDir = logDir;
      result.logFile = new File( logDir, "drillbit.log" );
      if ( result.logFile.exists() ) {
        result.loadLog( );
      }
      else {
        result.logFile = null;
      }
    }
  }

  public static class DrillbitRun extends ScriptRunner
  {
    public File pidDir;

    public DrillbitRun( ) {
      super( "drillbit.sh" );
    }

    public DrillbitRun( String cmd ) {
      super( "drillbit.sh", cmd );
    }

    public DrillbitRun withPidDir( File pidDir ) {
      this.pidDir = pidDir;
      return this;
    }

    public DrillbitRun asDaemon( )
    {
      addEnv( "KEEP_RUNNING", "1" );
      return this;
    }

    public RunResult start( ) throws IOException {
      if ( pidDir == null ) {
        pidDir = drillHome;
      }
      pidFile = new File( pidDir, "drillbit.pid" );
//      pidFile.delete();
      asDaemon( );
      RunResult result = run( );
      if ( result.returnCode == 0 ) {
        capturePidFile( result );
        captureDrillOut( result );
      }
      return result;
    }

    private void capturePidFile( RunResult result ) {
      assertTrue( pidFile.exists() );
      result.pidFile = pidFile;
    }

    private void captureDrillOut( RunResult result ) throws IOException {
      // Drillbit.out

      result.outFile = new File( result.logDir, "drillbit.out" );
      if ( result.outFile.exists() ) {
        result.loadOut( );
      }
      else {
        result.outFile = null;
      }
    }

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
    File siteDir = new File( testDrillHome, "conf" );
    createMockConf( siteDir );

    // No drill-env.sh, no distrib-env.sh

    {
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .run( );
      assertEquals( 0, result.returnCode );
      result.validateJava( );
      result.validateStockArgs( );
      result.validateClassPath( stdCp );
      result.validateStdOut( );
      result.validateStdErr( );
      result.validateDrillLog( );
    }

    // As above, but pass an argument.

    {
      String propArg = "-Dproperty=value";
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .withArg( propArg )
          .run( );
      assertEquals( 0, result.returnCode );
      result.validateStdOut( );
      result.validateArg( propArg );
    }

    // Custom Java opts to achieve the same result

    {
      String propArg = "-Dproperty=value";
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .addEnv( "DRILL_JAVA_OPTS", propArg )
          .run( );
      assertEquals( 0, result.returnCode );
      result.validateStockArgs( ); // Should not lose standard JVM args
      result.validateStdOut( );
      result.validateArg( propArg );
    }

    // Custom Drillbit Java Opts to achieve the same result

    {
      String propArg = "-Dproperty2=value2";
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .addEnv( "DRILLBIT_JAVA_OPTS", propArg )
          .run( );
      assertEquals( 0, result.returnCode );
      result.validateStockArgs( ); // Should not lose standard JVM args
      result.validateStdOut( );
      result.validateArg( propArg );
    }

    // Both sets of options

    {
      String propArg = "-Dproperty=value";
      String propArg2 = "-Dproperty2=value2";
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .addEnv( "DRILL_JAVA_OPTS", propArg )
          .addEnv( "DRILLBIT_JAVA_OPTS", propArg2 )
          .run( );
      assertEquals( 0, result.returnCode );
      result.validateArgs( new String[] { propArg, propArg2 } );
    }

    // Custom heap memory

    {
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .addEnv( "DRILL_HEAP", "5G" )
          .run( );
      result.validateArgs( new String[] { "-Xms5G", "-Xmx5G" } );
    }

    // Custom direct memory

    {
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .addEnv( "DRILL_MAX_DIRECT_MEMORY", "7G" )
          .run( );
      result.validateArg( "-XX:MaxDirectMemorySize=7G" );
    }

    // Enable GC logging

    {
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .addEnv( "SERVER_LOG_GC", "1" )
          .run( );
      String logTail = testDrillHome.getName() + "/log/drillbit.gc";
      result.validateArgRegex( "-Xloggc:.*/" + logTail );
    }

    // Max Perm Size

    {
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .addEnv( "DRILLBIT_MAX_PERM", "600M" )
          .run( );
      result.validateArg( "-XX:MaxPermSize=600M" );
    }

    // Code cache size

    {
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .addEnv( "DRILLBIT_CODE_CACHE_SIZE", "2G" )
          .run( );
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
    File siteDir = new File( testDrillHome, "conf" );
    createMockConf( siteDir );

    File extrasDir = createDir( new File( testDir, "extras" ) );
    File hadoopJar = makeDummyJar( extrasDir, "hadoop" );
    File hbaseJar = makeDummyJar( extrasDir, "hbase" );
    File prefixJar = makeDummyJar( extrasDir, "prefix" );
    File cpJar = makeDummyJar( extrasDir, "cp" );
    File extnJar = makeDummyJar( extrasDir, "extn" );
    File toolsJar = makeDummyJar( extrasDir, "tools" );

    {
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .addEnv( "DRILL_CLASSPATH_PREFIX", prefixJar.getAbsolutePath() )
          .run( );
      result.validateClassPath( prefixJar.getAbsolutePath() );
    }

    {
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .addEnv( "DRILL_TOOL_CP", toolsJar.getAbsolutePath() )
          .run( );
      result.validateClassPath( toolsJar.getAbsolutePath() );
    }

    {
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .addEnv( "HADOOP_CLASSPATH", hadoopJar.getAbsolutePath() )
          .run( );
      result.validateClassPath( hadoopJar.getAbsolutePath() );
    }

    {
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .addEnv( "HBASE_CLASSPATH", hbaseJar.getAbsolutePath() )
          .run( );
      result.validateClassPath( hbaseJar.getAbsolutePath() );
    }

    {
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .addEnv( "EXTN_CLASSPATH", extnJar.getAbsolutePath() )
          .run( );
      result.validateClassPath( extnJar.getAbsolutePath() );
    }

    {
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .addEnv( "DRILL_CLASSPATH", cpJar.getAbsolutePath() )
          .run( );
      result.validateClassPath( cpJar.getAbsolutePath() );
    }

    // Site jars not on path if not created

    File siteJars = new File( siteDir, "jars" );
    {
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .run( );
      assertFalse( result.classPathContains( siteJars.getAbsolutePath() ) );
    }

    // Site/jars on path if exists

    createDir( siteJars );
    makeDummyJar( siteJars, "site" );

    {
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .run( );
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
    File siteDir = new File( testDrillHome, "conf" );
    createMockConf( siteDir );
    File logsDir = createDir( new File( testDir, "logs" ) );
    removeDir( new File( testDrillHome, "log" ) );

    {
      String logPath = logsDir.getAbsolutePath();
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .addEnv( "DRILL_LOG_DIR", logPath )
          .withLogDir( logsDir )
          .run( );
      assertEquals( 0, result.returnCode );
      result.validateArgs( new String[] {
          "-Dlog.path=" + logPath + "/drillbit.log",
          "-Dlog.query.path=" + logPath + "/drillbit_queries.json",
      } );
      result.validateStdOut( );
      result.validateStdErr( );
      result.validateDrillLog( );
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
    File siteDir = new File( testDrillHome, "conf" );
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
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .run( );
      assertEquals( 0, result.returnCode );

      String expectedArgs[] = {
          propArg,
          "-Xms5G", "-Xmx5G",
          "-XX:MaxDirectMemorySize=7G",
          "-XX:ReservedCodeCacheSize=2G",
          "-XX:MaxPermSize=600M"
      };

      result.validateArgs( expectedArgs );
      String logTail = testDrillHome.getName() + "/log/drillbit.gc";
      result.validateArgRegex( "-Xloggc:.*/" + logTail );
    }

    // Change some drill-env.sh options in the environment.
    // The generated drill-env.sh should allow overrides.
    // (The generated form is the form that customers should use.)

    {
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .addEnv( "SERVER_LOG_GC", "0" )
          .addEnv( "DRILL_MAX_DIRECT_MEMORY", "9G" )
          .run( );
      assertEquals( 0, result.returnCode );
      result.validateArg( "-XX:MaxDirectMemorySize=9G" );
      result.validateArg( "-XX:MaxPermSize=600M" );
      String logTail = testDrillHome.getName() + "/log/drillbit.gc";
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
    File siteDir = new File( testDrillHome, "conf" );
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
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .run( );
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
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .addEnv( "DRILL_MAX_DIRECT_MEMORY", "5G" )
          .run( );
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
    createMockDistrib( );
    File siteDir = new File( testDrillHome, "conf" );
    removeDir( siteDir );

    // Directory does not exist.

    {
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .withSite( siteDir )
          .run( );
      assertEquals( 1, result.returnCode );
      assertTrue( result.stderr.contains( "Config dir does not exist" ) );
    }

    // Not a directory

    writeFile( siteDir, "dummy" );
    {
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .withSite( siteDir )
          .run( );
      assertEquals( 1, result.returnCode );
      assertTrue( result.stderr.contains( "Config dir does not exist" ) );
    }

    // Directory exists, but drill-override.conf does not

    siteDir.delete( );
    createDir( siteDir );
    {
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .withSite( siteDir )
          .run( );
      assertEquals( 1, result.returnCode );
      assertTrue( result.stderr.contains( "Drill config file missing" ) );
    }
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
    File confDir = new File( testDrillHome, "conf" );
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
      // Use --config explicitly

      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .withArg( "--config" )
          .withArg( siteDir.getAbsolutePath() )
          .run( );
      assertEquals( 0, result.returnCode );
      result.validateArgs(expectedArgs );
      result.validateClassPath( siteDir.getAbsolutePath() );
    }

    {
      RunResult result = new DrillbitRun( )
          .withArg( "--config" )
          .withArg( siteDir.getAbsolutePath() )
          .withArg( DRILLBIT_RUN )
          .run( );
      assertEquals( 0, result.returnCode );
      result.validateArgs(expectedArgs );
    }

    {
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .withSite( siteDir )
          .run( );
      assertEquals( 0, result.returnCode );
      result.validateArgs(expectedArgs );
    }

    // Site argument and argument to Drillbit

    {
      String propArg = "-Dproperty=value";
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .withSite( siteDir )
          .withArg( propArg )
          .run( );
      assertEquals( 0, result.returnCode );
      result.validateArgs(expectedArgs );
      result.validateArg( propArg );
    }

    // Set as an environment variable

    {
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .addEnv( "DRILL_CONF_DIR", siteDir.getAbsolutePath() )
          .run( );
      assertEquals( 0, result.returnCode );
      result.validateArgs(expectedArgs );
    }

    // Site jars not on path if not created

    {
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .withSite( siteDir )
          .run( );
      assertFalse( result.classPathContains( siteJars.getAbsolutePath() ) );
    }

    // Site/jars on path if exists

    createDir( siteJars );
    makeDummyJar( siteJars, "site" );

    {
      RunResult result = new DrillbitRun( DRILLBIT_RUN )
          .withSite( siteDir )
          .run( );
      assertTrue( result.classPathContains( siteJars.getAbsolutePath() + "/*" ) );
    }
  }

  public void copyFile( File source, File dest ) throws IOException {
    Files.copy( source.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING );
  }

  /**
   * Test running a (simulated) Drillbit as a daemon with start, status, stop.
   *
   * @throws IOException
   */

  @Test
  public void testStockDaemon( ) throws IOException
  {
    createMockDistrib( );
    File siteDir = new File( testDrillHome, "conf" );
    createMockConf( siteDir );

    // No drill-env.sh, no distrib-env.sh

    File pidFile;
    {
      RunResult result = new DrillbitRun( DRILLBIT_START )
          .start( );
      assertEquals( 0, result.returnCode );
      result.validateJava( );
      result.validateStockArgs( );
      result.validateClassPath( stdCp );
      assertTrue( result.stdout.contains( "Starting drillbit, logging") );
      assertTrue( result.log.contains( "Starting drillbit on" ) );
      assertTrue( result.log.contains( "Drill Log Message" ) );
      assertTrue( result.out.contains( "Drill Stdout Message" ) );
      assertTrue( result.out.contains( "Stderr Message" ) );
      pidFile = result.pidFile;
    }

    // Save the pid file for reuse.

    assertTrue( pidFile.exists() );
    File saveDir = new File( testDir, "save" );
    createDir( saveDir );
    File savedPidFile = new File( saveDir, pidFile.getName( ) );
    copyFile( pidFile, savedPidFile );

    // Status should be running

    {
      RunResult result = new DrillbitRun( DRILLBIT_STATUS )
          .run( );
      assertEquals( 0, result.returnCode );
      assertTrue( result.stdout.contains( "drillbit is running" ) );
    }

    // Start should refuse to start a second Drillbit.

    {
      RunResult result = new DrillbitRun( DRILLBIT_START )
          .start( );
      assertEquals( 1, result.returnCode );
      assertTrue( result.stdout.contains( "drillbit is already running as process" ) );
    }

    // Normal start, allow normal shutdown

    {
      RunResult result = new DrillbitRun( DRILLBIT_STOP )
          .run( );
      assertEquals( 0, result.returnCode );
      assertTrue( result.log.contains( "Terminating drillbit pid" ) );
      assertTrue( result.stdout.contains( "Stopping drillbit" ) );
    }

    // Status should report no drillbit (no pid file)

    {
      RunResult result = new DrillbitRun( DRILLBIT_STATUS )
          .run( );
      assertEquals( 1, result.returnCode );
      assertTrue( result.stdout.contains( "drillbit is not running" ) );
    }

    // Stop should report no pid file

    {
      RunResult result = new DrillbitRun( DRILLBIT_STOP )
          .run( );
      assertEquals( 1, result.returnCode );
      assertTrue( result.stdout.contains( "No drillbit to stop because no pid file" ) );
    }

    // Get nasty. Put the pid file back. But, there is no process with that pid.

    copyFile( savedPidFile, pidFile );

    // Status should now complain.

    {
      RunResult result = new DrillbitRun( DRILLBIT_STATUS )
          .run( );
      assertEquals( 1, result.returnCode );
      assertTrue( result.stdout.contains( "file is present but drillbit is not running" ) );
    }

    // As should stop.

    {
      RunResult result = new DrillbitRun( DRILLBIT_STOP )
          .run( );
      assertEquals( 1, result.returnCode );
      assertTrue( result.stdout.contains( "No drillbit to stop because kill -0 of pid" ) );
    }
  }

  @Test
  public void testStockDaemonWithArg( ) throws IOException
  {
    createMockDistrib( );
    File siteDir = new File( testDrillHome, "conf" );
    createMockConf( siteDir );

    // As above, but pass an argument.

    {
      String propArg = "-Dproperty=value";
      DrillbitRun runner = new DrillbitRun( DRILLBIT_START );
      runner.withArg( propArg );
      RunResult result = runner.start( );
      assertEquals( 0, result.returnCode );
      result.validateArg( propArg );
    }

    validateAndCloseDaemon( null );
  }

  private void validateAndCloseDaemon( File siteDir ) throws IOException
  {
    {
      RunResult result = new DrillbitRun( DRILLBIT_STATUS )
          .withSite( siteDir )
          .run( );
      assertEquals( 0, result.returnCode );
      assertTrue( result.stdout.contains( "drillbit is running" ) );
    }

    {
      RunResult result = new DrillbitRun( DRILLBIT_STOP )
          .withSite( siteDir )
          .run( );
      assertEquals( 0, result.returnCode );
    }
  }

  /**
   * The Daemon process creates a pid file. Verify that the DRILL_PID_DIR can
   * be set to put the pid file in a custom location. The test is done with
   * the site (conf) dir in the default location.
   *
   * @throws IOException
   */

  @Test
  public void testPidDir( ) throws IOException
  {
    createMockDistrib( );
    File siteDir = new File( testDrillHome, "conf" );
    createMockConf( siteDir );
    File pidDir = createDir( new File( testDir, "pid" ) );
    Map<String,String> drillEnv = new HashMap<>( );
    drillEnv.put( "DRILL_PID_DIR", pidDir.getAbsolutePath() );
    createEnvFile( new File( siteDir, "drill-env.sh" ), drillEnv );

    {
      RunResult result = new DrillbitRun( DRILLBIT_START )
          .withPidDir( pidDir )
          .start( );
      assertEquals( 0, result.returnCode );
      assertTrue( result.pidFile.getParentFile().equals( pidDir ) );
      assertTrue( result.pidFile.exists() );
    }

    validateAndCloseDaemon( null );
  }

  /**
   * Test a custom site directory with the Drill daemon process.
   * The custom directory contains a drill-env.sh with a custom
   * option. Verify that that option is picked up when starting Drill.
   *
   * @throws IOException
   */

  @Test
  public void testSiteDirWithDaemon( ) throws IOException
  {
    createMockDistrib( );

    File siteDir = new File( testDir, "site" );
    createMockConf( siteDir );

    Map<String,String> drillEnv = new HashMap<>( );
    drillEnv.put( "DRILL_MAX_DIRECT_MEMORY", "9G" );
    createEnvFile( new File( siteDir, "drill-env.sh" ), drillEnv );

    // Use the -site (--config) option.

    {
      DrillbitRun runner = new DrillbitRun( DRILLBIT_START );
      runner.withSite( siteDir );
      RunResult result = runner.start( );
      assertEquals( 0, result.returnCode );
      result.validateArg( "-XX:MaxDirectMemorySize=9G" );
    }

    validateAndCloseDaemon( siteDir );

    // Set an env var.

    {
      DrillbitRun runner = new DrillbitRun( DRILLBIT_START );
      runner.addEnv( "DRILL_CONF_DIR", siteDir.getAbsolutePath() );
      RunResult result =  runner.start( );
      assertEquals( 0, result.returnCode );
      result.validateArg( "-XX:MaxDirectMemorySize=9G" );
    }

    validateAndCloseDaemon( siteDir );
  }

  /**
   * Launch the Drill daemon using a custom log file location.
   * The config is in the default location.
   *
   * @throws IOException
   */

  @Test
  public void testLogDirWithDaemon( ) throws IOException
  {
    createMockDistrib( );
    File siteDir = new File( testDrillHome, "conf" );
    createMockConf( siteDir );
    File logsDir = createDir( new File( testDir, "logs" ) );
    removeDir( new File( testDrillHome, "log" ) );
    Map<String,String> drillEnv = new HashMap<>( );
    drillEnv.put( "DRILL_LOG_DIR", logsDir.getAbsolutePath() );
    createEnvFile( new File( siteDir, "drill-env.sh" ), drillEnv );

    {
      DrillbitRun runner = new DrillbitRun( DRILLBIT_START );
      runner.withLogDir( logsDir );
      RunResult result = runner.start( );
      assertEquals( 0, result.returnCode );
      assertNotNull( result.logFile );
      assertTrue( result.logFile.getParentFile().equals( logsDir ) );
      assertTrue( result.logFile.exists() );
      assertNotNull( result.outFile );
      assertTrue( result.outFile.getParentFile().equals( logsDir ) );
      assertTrue( result.outFile.exists() );
    }

    validateAndCloseDaemon( null );
  }

  /**
   * Some distributions create symlinks to drillbit.sh in standard locations
   * such as /usr/bin. Because drillbit.sh uses its own location to compute
   * DRILL_HOME, it must handle symlinks. This test verifies that process.
   *
   * @throws IOException
   */

  @Test
  public void testDrillbitSymlink( ) throws IOException
  {
    createMockDistrib( );
    File siteDir = new File( testDrillHome, "conf" );
    createMockConf( siteDir );

    File drillbitFile = new File( testDrillHome, "bin/drillbit.sh" );
    File linksDir = createDir( new File( testDir, "links" ) );
    File link = new File( linksDir, drillbitFile.getName( ) );
    try {
      Files.createSymbolicLink(link.toPath(), drillbitFile.toPath() );
    }
    catch ( UnsupportedOperationException e ) {
      // Well. This is a system without symlinks, so we won't be testing
      // syminks here...

      return;
    }

    {
      RunResult result = new DrillbitRun( DRILLBIT_START )
          .start( );
      assertEquals( 0, result.returnCode );
      assertEquals( result.pidFile.getParentFile( ), testDrillHome );
    }
    validateAndCloseDaemon( null );
  }

  /**
   * Test the restart command of drillbit.sh
   * @throws IOException
   */

  @Test
  public void testRestart( ) throws IOException
  {
    createMockDistrib( );
    File siteDir = new File( testDrillHome, "conf" );
    createMockConf( siteDir );

    int firstPid;
    {
      RunResult result = new DrillbitRun( DRILLBIT_START )
          .start( );
      assertEquals( 0, result.returnCode );
      firstPid = result.getPid( );
    }

    // Make sure it is running.

    {
      RunResult result = new DrillbitRun( DRILLBIT_STATUS )
          .withSite( siteDir )
          .run( );
      assertEquals( 0, result.returnCode );
      assertTrue( result.stdout.contains( "drillbit is running" ) );
    }

    // Restart. Should get new pid.

    {
      RunResult result = new DrillbitRun( DRILLBIT_RESTART )
          .start( );
      assertEquals( 0, result.returnCode );
      int secondPid = result.getPid( );
      assertNotEquals( firstPid, secondPid );
    }

    validateAndCloseDaemon( null );
  }

  /**
   * Simulate a Drillbit that refuses to die. The stop script wait a while,
   * then forces killing.
   *
   * @throws IOException
   */

  @Test
  public void testForcedKill( ) throws IOException
  {
    createMockDistrib( );
    File siteDir = new File( testDrillHome, "conf" );
    createMockConf( siteDir );

    {
      DrillbitRun runner = new DrillbitRun( DRILLBIT_START );
      runner.addEnv( "PRETEND_HUNG", "1");
      RunResult result = runner.start( );
      assertEquals( 0, result.returnCode );
    }

    {
      RunResult result = new DrillbitRun( DRILLBIT_STATUS )
          .preserveLogs( )
          .run( );
      assertEquals( 0, result.returnCode );
      assertTrue( result.stdout.contains( "drillbit is running" ) );
    }

    {
      RunResult result = new DrillbitRun( DRILLBIT_STOP )
          .addEnv( "DRILL_STOP_TIMEOUT", "5" )
          .preserveLogs( )
          .run( );
      assertEquals( 0, result.returnCode );
      assertTrue( result.stdout.contains( "drillbit did not complete after" ) );
    }
  }

  // Fixed locations (may be hard to test)

  /**
   * Out-of-the-box command-line arguments when launching sqlline.
   * Order is not important here (though it is to Java.)
   */

  public static String sqlLineArgs[] = {
      "-Dlog.path=/.*/drill/log/sqlline\\.log",
      "-Dlog.query.path=/.*/drill/log/sqlline_queries\\.json",
      "-XX:MaxPermSize=512M",
      "sqlline\\.SqlLine",
      "-d",
      "org\\.apache\\.drill\\.jdbc\\.Driver",
      "--maxWidth=10000",
      "--color=true"
  };

  /**
   * Verify the basics of the sqlline script, including the
   * env vars that can be customized. Also validate running
   * in embedded mode (using drillbit memory and other options.)
   * @throws IOException
   */

  @Test
  public void testSqlline( ) throws IOException
  {
    createMockDistrib( );
    File siteDir = new File( testDrillHome, "conf" );
    createMockConf( siteDir );

    int stockArgCount;
    {
      // Out-of-the-box sqlline

      RunResult result = new ScriptRunner( "sqlline" ).run( );
      assertEquals( 0, result.returnCode );
      result.validateJava( );
      System.out.println( "-- Class path --" );
      for ( String str : result.classPath ) {
        System.out.println( str );
      }
      System.out.println( "-- args --" );
      for ( String str : result.echoArgs ) {
        System.out.println( str );
      }
      result.validateClassPath( stdCp );
      assertTrue( result.containsArgsRegex( sqlLineArgs ) );
      stockArgCount = result.echoArgs.size();
    }

    {
      RunResult result = new ScriptRunner( "sqlline" )
          .withArg( "arg1" )
          .withArg( "arg2" )
          .run( );
      assertTrue( result.containsArg( "arg1" ) );
      assertTrue( result.containsArg( "arg2" ) );
    }
    {
      // Change drill memory and other drill-specific
      // settings: should not affect sqlline

      Map<String,String> drillEnv = new HashMap<>( );
      drillEnv.put( "DRILL_JAVA_OPTS", "-Dprop=value" );
      drillEnv.put( "DRILL_HEAP", "5G" );
      drillEnv.put( "DRILL_MAX_DIRECT_MEMORY", "7G" );
      drillEnv.put( "SERVER_LOG_GC", "1" );
      drillEnv.put( "DRILLBIT_MAX_PERM", "600M" );
      drillEnv.put( "DRILLBIT_CODE_CACHE_SIZE", "2G" );
      RunResult result = new ScriptRunner( "sqlline" )
          .withEnvironment( drillEnv )
          .run( );
      assertTrue( result.containsArgsRegex( sqlLineArgs ) );

      // Nothing new should have been added

      assertEquals( stockArgCount, result.echoArgs.size() );
    }
    {
      // Change client memory: should affect sqlline

      Map<String,String> shellEnv = new HashMap<>( );
      shellEnv.put( "CLIENT_GC_OPTS", "-XX:+UseG1GC" );
      shellEnv.put( "SQLLINE_JAVA_OPTS", "-XX:MaxPermSize=256M" );
      RunResult result = new ScriptRunner( "sqlline" )
          .withEnvironment( shellEnv )
          .run( );
      assertTrue( result.containsArg( "-XX:MaxPermSize=256M" ) );
      assertTrue( result.containsArg( "-XX:+UseG1GC" ) );
    }
    {
      // Change drill memory and other drill-specific
      // settings: then set the "magic" variable that says
      // that Drill is embedded. The scripts should now use
      // the Drillbit options.

      Map<String,String> drillEnv = new HashMap<>( );
      drillEnv.put( "DRILL_JAVA_OPTS", "-Dprop=value" );
      drillEnv.put( "DRILL_HEAP", "5G" );
      drillEnv.put( "DRILL_MAX_DIRECT_MEMORY", "7G" );
      drillEnv.put( "SERVER_LOG_GC", "1" );
      drillEnv.put( "DRILLBIT_MAX_PERM", "600M" );
      drillEnv.put( "DRILLBIT_CODE_CACHE_SIZE", "2G" );
      drillEnv.put( "DRILL_EMBEDDED", "1" );
      RunResult result = new ScriptRunner( "sqlline" )
          .withEnvironment( drillEnv )
          .run( );

      String expectedArgs[] = {
          "-Dprop=value",
          "-Xms5G", "-Xmx5G",
          "-XX:MaxDirectMemorySize=7G",
          "-XX:ReservedCodeCacheSize=2G",
          "-XX:MaxPermSize=600M"
      };

      result.validateArgs( expectedArgs );
      assertTrue( result.containsArg( "sqlline.SqlLine" ) );
    }
  }

  /**
   * Verify that the sqlline client works with the --site
   * option by customizing items in the site directory.
   *
   * @throws IOException
   */

  @Test
  public void testSqllineSiteDir( ) throws IOException
  {
    createMockDistrib( );
    File siteDir = new File( testDir, "site" );
    createMockConf( siteDir );

    // Dummy drill-env.sh to simulate the shipped "example" file
    // with some client-specific changes.

    writeFile( new File( siteDir, "drill-env.sh" ),
        "#!/usr/bin/env bash\n" +
        "# Example file\n" +
        "export SQLLINE_JAVA_OPTS=\"-XX:MaxPermSize=256M\"\n"
        );
    File siteJars = new File( siteDir, "jars" );
    createDir( siteJars );
    makeDummyJar( siteJars, "site" );
    {
      RunResult result = new ScriptRunner( "sqlline" )
          .withSite(siteDir)
          .run( );
      assertEquals( 0, result.returnCode );
      assertTrue( result.containsArg( "-XX:MaxPermSize=256M" ) );
      result.validateClassPath( siteJars.getAbsolutePath() + "/*" );
    }
  }

  /**
   * Tests the three scripts that wrap sqlline for specific purposes:
   * <ul>
   * <li>drill-conf — Wrapper for sqlline, uses drill config to find Drill.
   * Seems this one needs fixing to use a config other than the hard-coded
   * $DRILL_HOME/conf location.</li>
   * <li>drill-embedded — Starts a drill “embedded” in SqlLine, using a local ZK.</li>
   * <li>drill-localhost — Wrapper for sqlline, uses a local ZK.</li>
   * </ul>
   *
   * Of these, drill-embedded runs an embedded Drillbit and so should use the
   * Drillbit memory options. The other two are clients, but with simple
   * default options for finding the Drillbit.
   * <p>
   * Because the scripts are simple wrappers, all we do is verify that the right
   * "extra" options are set, not the fundamentals (which were already covered
   * in the sqlline tests.)
   *
   * @throws IOException
   */

  @Test
  public void testSqllineWrappers( ) throws IOException
  {
    createMockDistrib( );
    File siteDir = new File( testDrillHome, "conf" );
    createMockConf( siteDir );

    {
      // drill-conf: just adds a stub JDBC connect string.

      RunResult result = new ScriptRunner( "drill-conf" )
          .withArg( "arg1" )
          .run( );
      assertEquals( 0, result.returnCode );
      result.validateJava( );
      result.validateClassPath( stdCp );
      assertTrue( result.containsArgsRegex( sqlLineArgs ) );
      assertTrue( result.containsArg( "-u" ) );
      assertTrue( result.containsArg( "jdbc:drill:" ) );
      assertTrue( result.containsArg( "arg1" ) );
    }

    {
      // drill-localhost: Adds a JDBC connect string to a drillbit
      // on the localhost

      RunResult result = new ScriptRunner( "drill-localhost" )
          .withArg( "arg1" )
          .run( );
      assertEquals( 0, result.returnCode );
      result.validateJava( );
      result.validateClassPath( stdCp );
      assertTrue( result.containsArgsRegex( sqlLineArgs ) );
      assertTrue( result.containsArg( "-u" ) );
      assertTrue( result.containsArg( "jdbc:drill:drillbit=localhost" ) );
      assertTrue( result.containsArg( "arg1" ) );
   }

    {
      // drill-embedded: Uses drillbit startup options and
      // connects to the embedded drillbit.

      RunResult result = new ScriptRunner( "drill-embedded" )
          .withArg( "arg1" )
          .run( );
      assertEquals( 0, result.returnCode );
      result.validateJava( );
      result.validateClassPath( stdCp );
      assertTrue( result.containsArgsRegex( sqlLineArgs ) );
      assertTrue( result.containsArg( "-u" ) );
      assertTrue( result.containsArg( "jdbc:drill:zk=local" ) );
      assertTrue( result.containsArg( "-Xms4G" ) );
      assertTrue( result.containsArg( "-XX:MaxDirectMemorySize=8G" ) );
      assertTrue( result.containsArg( "arg1" ) );
    }
  }

}
