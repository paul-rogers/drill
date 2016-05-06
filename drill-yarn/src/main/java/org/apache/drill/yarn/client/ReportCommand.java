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

import org.apache.drill.yarn.core.DoYUtil;
import org.apache.drill.yarn.core.YarnClientException;
import org.apache.drill.yarn.core.YarnRMClient;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

public class ReportCommand extends ClientCommand
{
  public static class Reporter
  {
    private YarnRMClient client;
    ApplicationReport report;

    public Reporter( YarnRMClient client ) {
      this.client = client;
    }

    public void getReport( ) throws ClientException {
      try {
        report = client.getAppReport();
      } catch (YarnClientException e) {
        throw new ClientException( "Failed to get report for Drill application master", e );
      }
    }

    public void run( boolean verbose, boolean isNew ) throws ClientException {
      System.out.println("Application ID: " + client.getAppId().toString());
      getReport( );
      display( verbose, isNew );
    }

    public void display( boolean verbose, boolean isNew ) {
      if ( verbose || ! isNew ) {
        System.out.println( "Host: " + report.getHost() );
      }
      System.out.println( "Tracking URL: " + report.getTrackingUrl() );
      System.out.println( "Application Master URL: " + report.getOriginalTrackingUrl() );
      if ( verbose || ! isNew ) {
        System.out.println( "Application Name: " + report.getName() );
        System.out.println( "Queue: " + report.getQueue() );
        System.out.println( "User: " + report.getUser() );
        long startTime = report.getStartTime();
        System.out.println( "Start Time: " + DoYUtil.toIsoTime( startTime ) );
      }
      YarnApplicationState state = report.getYarnApplicationState();
      System.out.println( "State: " + state.toString() );
      if ( state == YarnApplicationState.FAILED  ||  state == YarnApplicationState.FINISHED ) {
        FinalApplicationStatus status = report.getFinalApplicationStatus();
        System.out.println( "Final status: " + status.toString() );
        if ( status != FinalApplicationStatus.SUCCEEDED ) {
          String diag = report.getDiagnostics();
          if ( ! DoYUtil.isBlank( diag ) ) {
            System.out.println( "Diagnostics: " + diag );
          }
        }
      }
    }

    public YarnApplicationState getState( ) {
      return report.getYarnApplicationState();
    }

    public boolean isStarting() {
      YarnApplicationState state = getState();
      return state == YarnApplicationState.ACCEPTED ||
             state == YarnApplicationState.NEW  ||
             state == YarnApplicationState.NEW_SAVING ||
             state == YarnApplicationState.SUBMITTED;
    }

    public boolean isStopped() {
      YarnApplicationState state = getState();
      return state == YarnApplicationState.FAILED  ||
             state == YarnApplicationState.FINISHED  ||
             state == YarnApplicationState.KILLED;
    }
  }

  @Override
  public void run() throws ClientException {
    new Reporter( getClient( ) ).run( opts.verbose, false );
  }
}
