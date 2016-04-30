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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.yarn.appMaster.Task;
import org.apache.drill.yarn.appMaster.TaskState;
import org.apache.drill.yarn.appMaster.TaskVisitor;
import org.apache.drill.yarn.core.DrillOnYarnConfig;
import org.apache.drill.yarn.zk.ZKRegistry;
import org.apache.hadoop.yarn.api.records.Container;

public class TasksModel implements TaskVisitor
{
  public static class TaskModel
  {
    public int id;
    public String poolName;
    public boolean isLive;
    public String state;
    public String trackingState;
    public Container container;
    public DrillbitEndpoint endpoint;
    public long startTime;
    public int memoryMb;
    public int vcores;

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

    public String getTrackingState( ) {
      return trackingState.toString();
    }

    public String getStartTime( ) {
      // Uses old-style dates rather than java.time becase
      // the code still must compile for JDK 7.

      DateFormat fmt = new SimpleDateFormat( "yyyy-MM-dd HH:mm" );
      fmt.setTimeZone(TimeZone.getDefault());
      return fmt.format( new Date( startTime ) );
    }

    public int getMemory( ) { return memoryMb; }
    public int getVcores( ) { return vcores; }
  }

  List<TaskModel> results = new ArrayList<>( );

  @Override
  public void visit(Task task) {
    results.add( toModel( task ) );
  }

  private TaskModel toModel( Task task ) {
    TaskModel model = new TaskModel( );
    model.id = task.taskId;
    model.poolName = task.scheduler.getName();
    TaskState state = task.getState();
    model.state = state.getLabel( );
    model.isLive = state == TaskState.RUNNING;
    model.trackingState = task.getTrackingState( ).toString();
    model.container = task.container;
    model.startTime = task.launchTime;
    if ( task.container != null ) {
      model.memoryMb = task.container.getResource().getMemory();
      model.vcores = task.container.getResource().getVirtualCores();
    }
    else {
      model.memoryMb = task.scheduler.getResource().memoryMb;
      model.vcores = task.scheduler.getResource().vCores;
    }
    model.endpoint = (DrillbitEndpoint) task.properties.get( ZKRegistry.ENDPOINT_PROPERTY );
    return model;
  }

}
