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


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.log4j.BasicConfigurator;

public class Client
{
  public static Config config;

  public static void main(String argv[]) {
    BasicConfigurator.configure();
    CommandLineOptions opts = new CommandLineOptions();
    opts.parse(argv);

    loadConfig( );

    ClientCommand cmd;
    switch (opts.getCommand()) {
      case TEST:
        cmd = new TestCommand(opts);
        break;
      case START:
        cmd = new StartCommand(opts);
        break;
      default:
        cmd = new HelpCommand(opts);
    }
    cmd.run();
  }

  private static void loadConfig( ) {
    config = ConfigFactory.load( "drill-on-yarn.conf" );
  }

}