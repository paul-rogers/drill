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

import org.apache.drill.yarn.core.YarnClientException;
import org.apache.drill.yarn.core.YarnRMClient;
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
        return; }
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


  @Override
  public void run() throws ClientException {
    YarnRMClient client = getClient( );
    System.out.println("Stopping Application ID: " + client.getAppId().toString());
    try {
      client.killApplication( );
    } catch (YarnClientException e) {
      throw new ClientException( "Failed to stop application master", e );
    }
    new StopMonitor( client ).run( opts.verbose );
    getAppIdFile().delete();
  }

}
