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

public abstract class AbstractDrillbitScheduler extends PersistentTaskScheduler
{
  // No way to gracefully shut down a Drillbit at present

  public class DrilbitManager extends AbstractTaskManager
  {
    /**
     * Allow only one concurrent container request by default to
     * ensure that the node blacklist mechanism works to ensure that
     * the RM does not allocate two containers on the same node.
     */

    @Override
    public int maxConcurrentAllocs() {return 1;}

    @Override
    public void allocated(EventContext context) {

      // One drillbit per node, so reserve the node
      // just allocated.

      context.controller.getNodeInventory( ).reserve( context.task.container );
    }

    @Override
    public void completed(EventContext context)
    {
      context.controller.getNodeInventory( ).release( context.task.container );
      analyzeResult( context );
    }

    /**
     * Analyze the result. Drillbits should not exit, but this one did.
     * It might be because we asked it to  exit, which is fine.
     * Otherwise, the exit is unexpected and we should 1) provide the
     * admin with an explanation, and 2) prevent retries after a few
     * tries.
     *
     * @param context
     */

    private void analyzeResult( EventContext context )
    {
      Task task = context.task;

      // If we cancelled the Drill-bit, just unblacklist the
      // host so we can run another drillbit on it later.

      if (task.isCancelled()) {return;}

      // The Drill-bit stopped on its own.
      // Maybe the exit status will tell us something.

      int exitCode = task.completionStatus.getExitStatus();

      // We can also consider the runtime.

      long duration = task.uptime() / 1000;

      // The ZK state may also help.

      boolean registered = task.trackingState != Task.TrackingState.NEW;

      // If the exit code was 1, then the script probably found
      // an error. Only retry once.

      if (registered || task.getTryCount() < 2) {

        // Use the default retry policy.

        return;
      }

      // Seems to be a mis-configuration. The Drill-bit exited quickly and
      // did not register in ZK. Also, we've tried twice now with no luck.
      // Assume the node is bad.

      String hostName = task.getHostName();
      StringBuilder buf = new StringBuilder( );
      buf.append( "Drillbit on host " )
         .append( hostName )
         .append( " failed with status " )
         .append( exitCode )
         .append( " after " )
         .append( duration )
         .append( "secs. with" );
      if ( ! registered ) {
        buf.append( "out" ); }
      buf.append( " ZK registration" );
      if ( duration < 60  &&  ! registered ) {
        buf.append( "\nProbable configuration problem, check Drill log file on host." ); }
      LOG.error( buf.toString() );
      task.cancelled = true;

      // Mark the host as permanently blacklisted. Leave it
      // in YARN's blacklist.

      context.controller.getNodeInventory( ).blacklist( hostName );
    }
  }

  private static final Log LOG = LogFactory.getLog(AbstractDrillbitScheduler.class);

  public AbstractDrillbitScheduler(String type, String name, int quantity) {
    super(type, name, quantity);
    isTracked = true;
    setTaskManager( new DrilbitManager( ) );
  }
}
