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
package org.apache.drill.yarn.appMaster.http;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.yarn.appMaster.ClusterController;
import org.apache.drill.yarn.appMaster.ClusterControllerImpl;
import org.apache.drill.yarn.appMaster.ControllerVisitor;
import org.apache.drill.yarn.appMaster.Task;
import org.apache.drill.yarn.appMaster.Task.TrackingState;
import org.apache.drill.yarn.appMaster.TaskState;
import org.apache.drill.yarn.appMaster.TaskVisitor;
import org.apache.drill.yarn.core.DoYUtil;
import org.apache.drill.yarn.core.DrillOnYarnConfig;
import org.apache.drill.yarn.zk.ZKRegistry;
import org.apache.hadoop.yarn.api.records.Container;

public abstract class AbstractTasksModel
{
  public static class TaskModel
  {
    public int id;
    protected String poolName;
    protected boolean isLive;
    protected TaskState taskState;
    protected String taskStateHint;
    protected String state;
    protected boolean cancelled;
    protected String trackingState;
    protected String trackingStateHint;
    protected Container container;
    protected DrillbitEndpoint endpoint;
    protected long startTime;
    protected int memoryMb;
    protected int vcores;
    protected String containerId;
    protected String nmLink;
    protected long endTime;
    protected String disposition;
    protected int tryCount;

    private Map<TaskState,String> stateHints = makeStateHints( );
    private Map<TrackingState,String> trackingStateHints = makeTrackingStateHints( );

    public TaskModel( Task task, boolean live )
    {
      id = task.taskId;
      poolName = task.scheduler.getName();
      taskState = task.getState();
      taskStateHint = stateHints.get( taskState );
      state = taskState.getLabel( );
      cancelled = task.isCancelled();
      isLive = live &&  taskState == TaskState.RUNNING;
      TrackingState tState = task.getTrackingState( );
      trackingState = tState.getDisplayName();
      trackingStateHint = trackingStateHints.get( tState );
      container = task.container;
      startTime = task.launchTime;
      if ( task.container != null ) {
        containerId = task.container.getId().toString();
        memoryMb = task.container.getResource().getMemory();
        vcores = task.container.getResource().getVirtualCores();

        // Emulate the NM link. Used for debugging, gets us to
        // the page on the NM UI for this container so we can see
        // logs, etc.

        nmLink = "http://" + task.container.getNodeHttpAddress();
      }
      else {
        memoryMb = task.scheduler.getResource().memoryMb;
        vcores = task.scheduler.getResource().vCores;
      }
      endpoint = (DrillbitEndpoint) task.properties.get( ZKRegistry.ENDPOINT_PROPERTY );
      if ( ! live ) {
        endTime = task.completionTime;
        tryCount = task.tryCount;

        // Determine disposition from most general to most
        // specific sources of information.

        disposition = state;
        if ( task.disposition != null ) {
          disposition = task.disposition.toString();
        }
        if ( task.completionStatus != null ) {
          disposition = task.completionStatus.getDiagnostics();
          if ( disposition != null ) {
            disposition = disposition.replace( "\n", "<br>\n" );
          }
        }
        if ( task.error != null ) {
          disposition = task.error.getMessage( );
        }
      }
    }

    private Map<TaskState, String> makeStateHints() {
      Map<TaskState, String> hints = new HashMap<>( );
      hints.put( TaskState.START, "Queued to send a container request to YARN." );
      hints.put( TaskState.REQUESTING, "Container request sent to YARN." );
      hints.put( TaskState.LAUNCHING, "YARN provided a container, send launch request." );
      hints.put( TaskState.WAIT_START_ACK, "Drillbit launched, waiting for ZooKeeper registration." );
      hints.put( TaskState.RUNNING, "Drillbit is running normally." );
      hints.put( TaskState.ENDING, "Graceful shutdown request sent to the Drillbit." );
      hints.put( TaskState.KILLING, "Sent the YARN Node Manager a request to forcefully kill the Drillbit." );
      hints.put( TaskState.WAIT_END_ACK, "Drillbit has shut down; waiting for ZooKeeper to confirm." );
      // The UI will never display the END state.
      hints.put( TaskState.END, "The Drillbit has shut down." );
      return hints;
    }

    private Map<TrackingState, String> makeTrackingStateHints() {
      Map<TrackingState, String> hints = new HashMap<>( );
      // UNTRACKED state is not used by Drillbits.
      hints.put( TrackingState.UNTRACKED, "Task is not tracked in ZooKeeper." );
      hints.put( TrackingState.NEW, "Drillbit has not yet registered with ZooKeeper." );
      hints.put( TrackingState.START_ACK, "Drillbit has registered normally with ZooKeeper." );
      hints.put( TrackingState.END_ACK, "Drillbit is no longer registered with ZooKeeper." );
      return hints;
    }

    public String getTaskId( ) {
      return Integer.toString( id );
    }

    public String getPoolName( ) { return poolName; }

    public boolean isLive( ) {
      return isLive;
    }

    public String getHost( ) {
      if ( container == null ) {
        return ""; }
      return container.getNodeId().getHost();
    }

    public String getLink( ) {
      if ( endpoint == null ) {
        return ""; }
      String port = DrillOnYarnConfig.config( ).getString( DrillOnYarnConfig.DRILLBIT_HTTP_PORT );
      String protocol = "http:";
      if ( DrillOnYarnConfig.config().getBoolean( DrillOnYarnConfig.DRILLBIT_USE_HTTPS ) ) {
        protocol = "https:";
      }
      return protocol + "//" + endpoint.getAddress() + ":" + port + "/";
    }

    public String getState( ) { return state.toString(); }
    public String getStateHint( ) { return taskStateHint; }
    public boolean isCancelled( ) { return cancelled; }

    public boolean isCancellable( ) {
      return ! cancelled  &&  taskState.isCancellable( );
    }

    public String getTrackingState( ) { return trackingState; }
    public String getTrackingStateHint( ) { return trackingStateHint; }

    public String getStartTime( ) {
      if ( startTime == 0 ) {
        return ""; }
      return DoYUtil.toIsoTime( startTime );
    }

    public int getMemory( ) { return memoryMb; }
    public int getVcores( ) { return vcores; }
    public boolean hasContainer( ) { return containerId != null; }
    public String getContainerId( ) { return displayString( containerId ); }
    public String getNmLink( ) { return displayString( nmLink ); }
    public String getDisposition( ) { return displayString( disposition ); }
    public int getTryCount( ) { return tryCount; }
    public String displayString( String value ) { return (value == null) ? "" : value; }

    public String getEndTime( ) {
      if ( endTime == 0 ) {
        return ""; }
      return DoYUtil.toIsoTime( endTime );
    }
  }

  public static class UnmanagedDrillbitModel
  {
    protected String host;
    protected String ports;

    public UnmanagedDrillbitModel( String endpoint ) {
      String parts[] = endpoint.split( ":" );
      if ( parts.length < 4 ) {
        // Should never occur, but better save than sorry.

        host = endpoint;
        ports = "";
      }
      else {
        host = parts[0];
        List<String> thePorts = new ArrayList<>( );
        thePorts.add( parts[1] );
        thePorts.add( parts[2] );
        thePorts.add( parts[3] );
        ports = DoYUtil.join( ", ", thePorts );
      }
    }

    public String getHost( ) { return host; }
    public String getPorts( ) { return ports; }
  }

  protected List<TaskModel> results = new ArrayList<>( );

  public List<TaskModel> getTasks( ) { return results; }
  public boolean hasTasks( ) { return ! results.isEmpty(); }

  public static class TasksModel extends AbstractTasksModel implements TaskVisitor
  {
    protected List<UnmanagedDrillbitModel> unmanaged;

    @Override
    public void visit(Task task) {
      results.add( new TaskModel( task, true ) );
    }

    /**
     * Sort tasks by Task ID.
     */

    public void sortTasks() {
      Collections.sort( results, new Comparator<TaskModel>( ) {
        @Override
        public int compare(TaskModel t1, TaskModel t2) {
          return Integer.compare( t1.id, t2.id );
        }
      });
    }

    /**
     * To avoid race conditions, do not use the controller visitor to invoke this method,
     * we want to leave the controller unlocked and instead lock only the ZK registry.
     *
     * @param controller
     */

    public void listUnmanaged(ClusterController controller) {
      ZKRegistry zkRegistry = (ZKRegistry) controller.getProperty( ZKRegistry.CONTROLLER_PROPERTY );
      if ( zkRegistry == null ) {
        return;
      }
      List<String> endpoints = zkRegistry.listUnmanagedDrillits( );
      if ( endpoints.isEmpty() ) {
        return; }
      unmanaged = new ArrayList<>( );
      for ( String endpoint : endpoints ) {
        unmanaged.add( new UnmanagedDrillbitModel( endpoint ) );
      }
    }

    public List<UnmanagedDrillbitModel>getUnnamaged( ) { return unmanaged; }
    public boolean hasUnmanagedDrillbits( ) { return unmanaged != null; }
    public int getUnmanagedDrillbitCount( ) {
      return (unmanaged == null) ? 0 : unmanaged.size( );
    }
  }

  public static class HistoryModel extends AbstractTasksModel implements ControllerVisitor
  {
    @Override
    public void visit(ClusterController controller) {
      ClusterControllerImpl impl = (ClusterControllerImpl) controller;
      for ( Task task : impl.getHistory( ) ) {
        results.add( new TaskModel( task, false ) );
      }
    }
  }
}
