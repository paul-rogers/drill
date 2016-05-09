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
import org.apache.drill.yarn.core.DrillOnYarnConfig;
import org.apache.drill.yarn.core.YarnClientException;
import org.apache.drill.yarn.core.YarnRMClient;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

public class StopCommand extends ClientCommand
{
  private static class StopMonitor
  {
    YarnRMClient client;
    ReportCommand.Reporter reporter;
    private YarnApplicationState state;

    StopMonitor( YarnRMClient client ) {
      this.client = client;
    }

    void run( boolean verbose ) throws ClientException {
      reporter = new ReportCommand.Reporter( client );
      reporter.getReport();
      if ( reporter.isStopped() ) {
        System.out.println( "Stopped." );
        return;
      }
      updateState( reporter.getState( ) );
      try {
        for ( int attempt = 0;  attempt < 15;  attempt++ )
        {
          if ( ! poll( ) ) {
            break; }
        }
      } finally {
        System.out.println( );
      }
      reporter.display( verbose, true );
      if ( reporter.isStopped() ) {
        System.out.println( "Stopped." );
      } else {
        System.out.println( "Application Master is slow to stop, use YARN to check status." );
      }
    }

    private boolean poll( ) throws ClientException {
      try {
        Thread.sleep( 1000 );
      } catch (InterruptedException e) {
        return false;
      }
      reporter.getReport( );
      if ( reporter.isStopped( ) ) {
        return false;
      }
      YarnApplicationState newState = reporter.getState( );
      if ( newState == state ) {
        System.out.print( "." );
        return true;
      }
      System.out.println( );
      updateState( newState );
      return true;
    }

    private void updateState( YarnApplicationState newState ) {
      state = newState;
      System.out.print( "Application State: " );
      System.out.println( state.toString( ) );
      System.out.print( "Stopping..." );
    }
  }

  private YarnRMClient client;

  @Override
  public void run() throws ClientException
  {
    YarnRMClient client = getClient( );
    System.out.println("Application ID: " + client.getAppId().toString());

    // First get an application report to ensure that the AM is,
    // in fact, running, and to get the HTTP endpoint.

    ApplicationReport report = getReport( );
    if ( report == null )
      return;

    // Try to stop the server by sending a STOP REST request.

    boolean stopped = gracefulStop( report );

    // If that did not work, then forcibly kill the AM.
    // YARN will forcibly kill the AM's containers.
    // Not pretty, but it works.

    if ( ! stopped )
      forcefulStop( );

    // Wait for the AM to stop.

    new StopMonitor( client ).run( opts.verbose );

    // The AM is gone. Forget its App Id.

    removeAppIdFile( );
  }

  private ApplicationReport getReport() {
    try {
      return client.getAppReport();
    } catch (YarnClientException e) {
      removeAppIdFile( );
      System.out.println( "Application is not running." );
      return null;
    }
  }

  private boolean gracefulStop( ApplicationReport report ) {
    try {
      String baseUrl = report.getOriginalTrackingUrl();
      if ( DoYUtil.isBlank( baseUrl ) ) {
        return false;
      }
      SimpleRestClient restClient = new SimpleRestClient( );
      String tail = "/rest/stop";
      String masterKey = DrillOnYarnConfig.config( ).getString( DrillOnYarnConfig.AM_REST_KEY );
      if ( ! DoYUtil.isBlank( masterKey ) )
        tail += "?key=" + masterKey;
      if ( opts.verbose )
        System.out.println( "Stopping with POST " + baseUrl + "/" + tail );
      String result = restClient.send( baseUrl, tail, true );
      if ( "OK".equals( result ) ) {
        return true;
      }
      System.err.println( "Failed to stop the application master. Response = " + result );
      return false;
    }
    catch ( ClientException e ) {
      System.err.println( e.getMessage() );
      System.out.println( "Resorting to forced kill" );
      return false;
    }
  }

  private void forcefulStop() throws ClientException {
    try {
      client.killApplication( );
    } catch (YarnClientException e) {
      throw new ClientException( "Failed to stop application master", e );
    }
  }
}
