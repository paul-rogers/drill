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
package org.apache.drill.yarn.appMaster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.drill.yarn.appMaster.http.WebServer;
import org.apache.drill.yarn.core.DrillOnYarnConfig;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Application Master for Drill.
 * <p>
 * To debug this AM use the customized unmanaged AM launcher in this
 * jar. (The "stock" YARN version does not give you time to attach
 * the debugger.)
 * <pre><code>
 * TARGET_JAR=/your-git-folder/drill-yarn/target/drill-yarn-1.6-SNAPSHOT.jar
 * TARGET_CLASS=org.apache.drill.yarn.appMaster.ApplicationMaster
 * LAUNCHER_JAR=$TARGET_JAR
 * LAUNCHER_CLASS=org.apache.drill.yarn.mock.UnmanagedAMLauncher
 * $HH/bin/hadoop jar $LAUNCHER_JAR \
 *   $LAUNCHER_CLASS -classpath $TARGET_JAR \
 *   -cmd "java -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 \
 *   $TARGET_CLASS"
 * </pre></code>
 */

public class ApplicationMaster
{
  private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

  public static void main(String[] args) {

    // Temporary

    Logger logger = Logger.getLogger("org.apache.drill.yarn");
    logger.setLevel(Level.TRACE);

    LOG.trace( "Drill Application Master starting." );

    // Load the configuration. Assumes that the user's Drill-on-YARN
    // configuration was archived along with the Drill software in
    // the $DRILL_HOME/conf directory, and that $DRILL_HOME/conf is
    // on the class-path.

    DrillOnYarnConfig.load();

    // Dispatcher am = (new SimpleBatchFactory( )).build( );
    // Dispatcher am = (new MockDrillbitFactory( )).build( );
    Dispatcher am;
    try {
      am = (new DrillbitFactory()).build();
    } catch (YarnFacadeException e) {
      LOG.error("Setup failed, exiting: " + e.getMessage(), e);
      System.exit(-1);
      return;
    }
    WebServer webServer = new WebServer( am );
    try {
      webServer.start();
    } catch (Exception e) {
      LOG.error("Web server setup failed, exiting: " + e.getMessage(), e);
      System.exit(-1);
    }
    try {
      am.run();
    } catch (Throwable e) {
      LOG.error("Fatal error, exiting: " + e.getMessage(), e);
      System.exit(-1);
    }
    finally {
      try {
        webServer.close( );
      } catch (Exception e) {
        // Ignore
      }
    }
  }
}
