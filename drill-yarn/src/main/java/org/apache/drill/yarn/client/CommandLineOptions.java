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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

/**
 * Drill YARN client command line options.
 * <p><pre>
 * DrillYarnClient -h|--help |
 *                 --start |
 *                 --stop |
 *                 --status |
 *                 --resize [+|-]n
 * </pre></p>
 * <ul>
 * <li>--help: Prints command line usage</li>
 * <li>--start: starts the defined cluster</li>
 * <li>--stop: stops the defined cluster</li>
 * <li>--resize: adds (+n), removes (-n) or resizes (n) the cluster</li>
 * <li>--status: prints status about the cluster</li>
 * </ul>
 */

public class CommandLineOptions {
  @Parameter(names = {"-h", "-?", "--help"}, description = "Provide description of usage.")
  private boolean help = false;

  @Parameter(names = {"--test"}, description = "Sanity test using simple-yarn-app AM.")
  private boolean test = false;

  /**
   * Primary command to upload the application archive and start the Drill cluster.
   */
  @Parameter(names = {"--start"}, description = "Start the cluster.")
  private boolean start = false;

  /**
   * Convenience method when debugging, testing. Restarts the cluster without the
   * archive upload; assumes the upload was already done.
   */
  @Parameter(names = {"--restart"}, description = "Restart the cluster (without archive upload).")
  private boolean restart = false;

  /**
   * Primary command to stop a running cluster.
   */
  @Parameter(names = {"--stop"}, description = "Stop the cluster.")
  private boolean stop = false;

  /**
   * Primary command to get the status of a running cluster.
   */
  @Parameter(names = {"--status"}, description = "Provide the status of the cluster.")
  private boolean status = false;

  @Parameter(names = {"--resize"}, description = "Resize the cluster +n: add nodes, -n: remove nodes, n resize to given size.")
  private String resize;
  private String prefix;
  int resizeValue;

  /**
   * Convenience command to display the effective configuration settings to
   * diagnose problems.
   */
  @Parameter(names = {"--dryrun"}, description = "Display and validate configuration.")
  private boolean dryRun = false;

  /**
   * Convenience command to upload the application archive to test the DFS
   * settings without launching the Drill cluster.
   */
  @Parameter(names = {"--upload"}, description = "Upload archives to validate DFS.")
  private boolean upload = false;

  @Parameter(names = {"-v", "--verbose"}, description = "Verbose output.")
  public boolean verbose = false;

  @Parameter(description = "Cluster name.")
  private List<String> clusters = new ArrayList<>();

  public static enum Command {
    ERROR, HELP, START, RESTART, STOP, STATUS, RESIZE, TEST, DRY_RUN, UPLOAD
  }

  Command command;

  private JCommander parser;

  /**
   * Parse the command line. Invalid option combinations result in the
   * error option being set.
   */
  public void parse(String argv[]) {
    try {
      parser = new JCommander(this, argv);
      validate();
    } catch (ParameterException e) {
      command = Command.ERROR;
    }
  }

  private void validate() {
    int count = 0;
    if (help) {
      count++;
    }
    if (start) {
      count++;
    }
    if (restart) {
      count++;
    }
    if (stop) {
      count++;
    }
    if (status) {
      count++;
    }
    if (resize != null) {
      count++;
    }
    if (test) {
      count++;
    }
    if (dryRun) {
      count++;
    }
    if (upload) {
      count++;
    }
    if (count != 1) {
      command = Command.ERROR;
      return;
    }

    if (clusters.size() > 1) {
      command = Command.ERROR;
      return;
    }

    if (count == 1 && resize != null) {
      Pattern p = Pattern.compile("([+-]?)(\\d+)");
      Matcher m = p.matcher(resize);
      if (m.matches()) {
        prefix = m.group(1);
        resizeValue = Integer.parseInt(m.group(2));
      } else {
        command = Command.ERROR;
        return;
      }
    }

    if (help) {
      command = Command.HELP;
    } else if (start) {
      command = Command.START;
    } else if (restart) {
      command = Command.RESTART;
    } else if (stop) {
      command = Command.STOP;
    } else if (status) {
      command = Command.STATUS;
    } else if (resize != null) {
      command = Command.RESIZE;
    } else if (test) {
      command = Command.TEST;
    } else if ( dryRun ) {
      command = Command.DRY_RUN;
    } else if ( upload ) {
      command = Command.UPLOAD;
    } else {
      command = Command.HELP;
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

  public String getCluster() {
    return clusters.isEmpty() ? null : clusters.get(0);
  }

  public void usage() {
    parser.usage();
  }

}
