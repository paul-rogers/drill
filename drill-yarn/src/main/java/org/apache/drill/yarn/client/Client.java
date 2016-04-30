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


import org.apache.drill.yarn.core.DrillOnYarnConfig;
import org.apache.log4j.BasicConfigurator;

/**
 * Client for the Drill-on-YARN integration. See YARN documentation
 * for the role of a YARN client.
 * <p>
 * To debug this class, add your conf folder to the class path
 * so that the config file can be found.
 */

public class Client
{
  public static void main(String argv[]) {
    BasicConfigurator.configure();
    CommandLineOptions opts = new CommandLineOptions();
    opts.parse(argv);

    DrillOnYarnConfig.load();
    // Debug only
    DrillOnYarnConfig.instance( ).dump( );

    ClientCommand cmd;
    switch (opts.getCommand()) {
      case TEST:
        cmd = new TestCommand( );
        break;
      case UPLOAD:
        cmd = new StartCommand( true, false );
        break;
      case START:
        cmd = new StartCommand( true, true );
        break;
      case RESTART:
        cmd = new StartCommand( false, true );
        break;
      case DRY_RUN:
        cmd = new DryRunCommand( );
        break;
      default:
        cmd = new HelpCommand( );
    }
    cmd.setOpts( opts );
    try {
      cmd.run();
    } catch (ClientException e) {
      System.err.println( e.getMessage() );
      System.exit( 1 );
    }
  }
}