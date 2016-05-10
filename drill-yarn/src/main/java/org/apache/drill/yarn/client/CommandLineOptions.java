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
package org.apache.drill.yarn.client;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Drill YARN client command line options.
 * <p><pre>
 * DrillYarnClient -h|--help |
 *                 start |
 *                 stop |
 *                 status |
 *                 resize [+|-]n
 * </pre></p>
 * <ul>
 * <li>help: Prints command line usage</li>
 * <li>start: starts the defined cluster</li>
 * <li>stop: stops the defined cluster</li>
 * <li>resize: adds (+n), removes (-n) or resizes (n) the cluster</li>
 * <li>status: prints status about the cluster</li>
 * </ul>
 * <p>
 * This is a do-it-yourself parser because the command line parser
 * used by Drill does not accept parameters (arguments) without a dash,
 * and does not accept arguments (such as resize -3) with a dash.
 */

public class CommandLineOptions {
  private String prefix;
  int resizeValue;

  public boolean verbose = false;

  public enum Command
  {
    HELP( "help", "Provide description of usage."),

    /**
     * Primary command to upload the application archive and start the Drill cluster.
     */

    START( "start", "Start the cluster."),

    /**
     * Convenience method when debugging, testing. Restarts the cluster without the
     * archive upload; assumes the upload was already done.
     */

    RESTART( "restart", "Restart the cluster (without archive upload)."),

    /**
     * Primary command to stop a running cluster.
     */

    STOP( "stop", "Stop the cluster."),

    /**
     * Primary command to get the status of a running cluster.
     */

    STATUS( "status", "Provide the status of the cluster."),

    RESIZE( "resize", "Resize the cluster +n: add nodes, -n: remove nodes, n resize to given size."),

    TEST( null, null ),

    /**
     * Convenience command to display the effective configuration settings to
     * diagnose problems.
     */

    DRY_RUN( "dryrun", "Display and validate configuration." ),

    /**
     * Convenience command to upload the application archive to test the DFS
     * settings without launching the Drill cluster.
     */

    UPLOAD( "upload", "Upload archives to validate DFS." );

    private String cmd;
    private String descrip;

    private Command( String cmd, String descrip ) {
      this.cmd = cmd;
      this.descrip = descrip;
    }

    public boolean isMatch( String arg ) {
      String key = (cmd == null) ? toString( ) : cmd;
      return key.equalsIgnoreCase( arg );
    }

    public boolean isHidden( ) {
      return descrip == null;
    }

    public String getCommand( ) { return cmd; }
    public String getDescription( ) { return descrip; }
  }

  Command command;

//  private JCommander parser;

  /**
   * Parse the command line. Invalid option combinations result in the
   * error option being set.
   */
  public void parse(String args[]) {
    for ( int i = 0;  i < args.length;  i++ ) {
      String arg = args[i];
      if ( arg.equals( "-h" ) ||  arg.equals( "-?" ) ) {
        command = Command.HELP;
        break;
      }
      if ( arg.equals( "-v" ) ||  arg.equals( "--verboase" ) ) {
        verbose = true;
        continue;
      }
      if ( command != null ) {
        command = null;
        return;
      }

      // Check if a command line word matches this command. Be nice,
      // allow -foo and --foo in addition to the "proper" foo.

      String cmdStr = arg;
      if ( cmdStr.startsWith( "--" ) ) {
        cmdStr = arg.substring( 2 );
      }
      else if ( cmdStr.startsWith( "-" ) ) {
        cmdStr = cmdStr.substring( 1 );
      }
      for ( Command cmd : Command.values() ) {
        if ( cmd.isMatch( cmdStr ) ) {
          command = cmd;
          if ( command == Command.RESIZE ) {
            if ( i + 1 == args.length ) {
              command = null;
              break;
            }
            parseResizeOption( args[++i] );
          }
          break;
        }
      }
    }
  }

  private void parseResizeOption( String resize ) {
    Pattern p = Pattern.compile("([+-]?)(\\d+)");
    Matcher m = p.matcher(resize);
    if (m.matches()) {
      prefix = m.group(1);
      resizeValue = Integer.parseInt(m.group(2));
    } else {
      command = null;
      return;
    }
  }

  public Command getCommand() {
    return command;
  }

  public String getResizePrefix() {
    return prefix;
  }

  public int getResizeValue() {
    return resizeValue;
  }

  public void usage() {
    System.out.println( "drill-on-yarn.sh [--hadoop hadoop-home][-v|--verbose] command args");
    System.out.println( "Where command is one of:" );
    for ( Command cmd : Command.values() ) {
      if ( cmd.isHidden( ) ) {
        continue; }
      System.out.println( "  " + cmd.getCommand( ) + " - " + cmd.getDescription( ) );
    }
  }
}
