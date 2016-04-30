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
import java.util.List;

import org.apache.drill.yarn.appMaster.ClusterController;
import org.apache.drill.yarn.appMaster.ClusterControllerImpl;
import org.apache.drill.yarn.appMaster.ContainerRequestSpec;
import org.apache.drill.yarn.appMaster.ClusterControllerImpl.State;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.drill.yarn.appMaster.ControllerVisitor;
import org.apache.drill.yarn.appMaster.Scheduler;
import org.apache.drill.yarn.appMaster.SchedulerStateActions;

public class ControllerModel implements ControllerVisitor
{
  public static class PoolModel
  {
    public String name;
    public String type;
    public int targetCount;
    public int taskCount;
    public int liveCount;
    public int memory;
    public int vcores;

    public String getName( ) { return name; }
    public String getType( ) { return type; }
    public int getTargetCount( ) { return targetCount; }
    public int getTaskCount( ) { return taskCount; }
    public int getLiveCount( ) { return liveCount; }
    public int getMemory( ) { return memory; }
    public int getVcores( ) { return vcores; }
  }

  public ClusterControllerImpl.State state;
  public int yarnMemory;
  public int yarnVcores;
  public int yarnNodeCount;
  public int taskCount;
  public int liveCount;
  public int targetCount;
  public int totalDrillMemory;
  public int totalDrillVcores;
  List<PoolModel> pools = new ArrayList<>( );

  public String getState( ) { return state.toString( ); }
  public int getYarnMemory( ) { return yarnMemory; }
  public int getYarnVcores( ) { return yarnVcores; }
  public int getDrillTotalMemory( ) { return totalDrillMemory; }
  public int getDrillTotalVcores( ) { return totalDrillVcores; }
  public int getYarnNodeCount( ) { return yarnNodeCount; }
  public int getTaskCount( ) { return taskCount; }
  public int getLiveCount( ) { return liveCount; }
  public int getTargetCount( ) { return targetCount; }
  public List<PoolModel> getPools( ) { return pools; }

  @Override
  public void visit(ClusterController controller) {
    ClusterControllerImpl impl = (ClusterControllerImpl) controller;
    state = impl.getState( );
    if ( state == State.LIVE ) {
      RegisterApplicationMasterResponse resp = impl.getYarn( ).getRegistrationResponse();
      yarnVcores = resp.getMaximumResourceCapability().getVirtualCores();
      yarnMemory = resp.getMaximumResourceCapability().getMemory();
      yarnNodeCount = impl.getYarn( ).getNodeCount();
    }
    for ( SchedulerStateActions pool : impl.getPools( ) ) {
      ControllerModel.PoolModel poolModel = new ControllerModel.PoolModel( );
      Scheduler sched = pool.getScheduler();
      ContainerRequestSpec containerSpec = sched.getResource( );
      poolModel.name = sched.getName();
      poolModel.type = sched.getType( );
      poolModel.targetCount = sched.getTarget();
      poolModel.memory = containerSpec.memoryMb;
      poolModel.vcores = containerSpec.vCores;
      poolModel.taskCount = pool.getTaskCount();
      poolModel.liveCount = pool.getLiveCount( );
      targetCount += poolModel.targetCount;
      taskCount += poolModel.taskCount;
      liveCount += poolModel.liveCount;
      totalDrillMemory += poolModel.liveCount * poolModel.memory;
      totalDrillVcores += poolModel.liveCount * poolModel.vcores;
      pools.add( poolModel );
    }
    if ( state != State.LIVE ) {
      targetCount = 0;
    }
  }

}
