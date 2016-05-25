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
import java.util.List;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.yarn.appMaster.ClusterController;
import org.apache.drill.yarn.appMaster.ClusterControllerImpl;
import org.apache.drill.yarn.appMaster.ControllerVisitor;
import org.apache.drill.yarn.appMaster.Task;
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
    public String poolName;
    public boolean isLive;
    public TaskState taskState;
    public String state;
    public boolean cancelled;
    public String trackingState;
    public Container container;
    public DrillbitEndpoint endpoint;
    public long startTime;
    public int memoryMb;
    public int vcores;
    public String containerId;
    public String nmLink;
    public long endTime;
    public String disposition;
    public int tryCount;

    public TaskModel( Task task, boolean live )
    {
      id = task.taskId;
      poolName = task.scheduler.getName();
      taskState = task.getState();
      state = taskState.getLabel( );
      cancelled = task.isCancelled();
      isLive = live &&  taskState == TaskState.RUNNING;
      trackingState = task.getTrackingState( ).toString();
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
        return null; }
      String port = DrillOnYarnConfig.config( ).getString( DrillOnYarnConfig.DRILLBIT_HTTP_PORT );
      return "http://" + endpoint.getAddress() + ":" + port + "/";
    }

    public String getState( ) {
      return state.toString();
    }

    public boolean isCancelled( ) {
      return cancelled;
    }

    public boolean isCancellable( ) {
      return ! cancelled  &&  taskState.isCancellable( );
    }

    public String getTrackingState( ) {
      return trackingState.toString();
    }

    public String getStartTime( ) {
      if ( startTime == 0 ) {
        return ""; }
      return DoYUtil.toIsoTime( startTime );
    }

    public int getMemory( ) { return memoryMb; }
    public int getVcores( ) { return vcores; }
    public boolean hasContainer( ) { return containerId != null; }
    public String getContainerId( ) { return containerId; }
    public String getNmLink( ) { return nmLink; }
    public String getDisposition( ) { return disposition; }
    public int getTryCount( ) { return tryCount; }

    public String getEndTime( ) {
      if ( endTime == 0 ) {
        return ""; }
      return DoYUtil.toIsoTime( endTime );
    }
  }

  List<TaskModel> results = new ArrayList<>( );

  public static class TasksModel extends AbstractTasksModel implements TaskVisitor
  {
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
