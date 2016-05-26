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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.drill.yarn.appMaster.AMYarnFacade.YarnAppHostReport;
import org.apache.drill.yarn.appMaster.ClusterController;
import org.apache.drill.yarn.appMaster.ClusterControllerImpl;
import org.apache.drill.yarn.appMaster.ContainerRequestSpec;
import org.apache.drill.yarn.appMaster.ClusterControllerImpl.State;
import org.apache.drill.yarn.core.DrillOnYarnConfig;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.drill.yarn.appMaster.ControllerVisitor;
import org.apache.drill.yarn.appMaster.Scheduler;
import org.apache.drill.yarn.appMaster.SchedulerStateActions;

@XmlRootElement
public class ControllerModel implements ControllerVisitor
{
  public static class PoolModel
  {
    protected String name;
    protected String type;
    protected int targetCount;
    protected int taskCount;
    protected int liveCount;
    protected int memory;
    protected int vcores;

    public String getName( ) { return name; }
    public String getType( ) { return type; }
    public int getTargetCount( ) { return targetCount; }
    public int getTaskCount( ) { return taskCount; }
    public int getLiveCount( ) { return liveCount; }
    public int getMemory( ) { return memory; }
    public int getVcores( ) { return vcores; }
  }

  protected ClusterControllerImpl.State state;
  protected String stateHint;
  protected int yarnMemory;
  protected int yarnVcores;
  protected int yarnNodeCount;
  protected int taskCount;
  protected int liveCount;
  protected int targetCount;
  protected int totalDrillMemory;
  protected int totalDrillVcores;
  protected YarnAppHostReport appRpt;
  protected List<PoolModel> pools = new ArrayList<>( );

  public String getAppId( ) { return appRpt.appId; }
  public String getRmHost( ) { return appRpt.rmHost; }
  public String getRmLink( ) { return appRpt.rmUrl; }
  public String getNmHost( ) { return appRpt.nmHost; }
  public String getNmLink( ) { return appRpt.nmUrl; }
  public String getRmAppLink( ) { return appRpt.rmAppUrl; }
  public String getNmAppLink( ) { return appRpt.nmAppUrl; }
  public String getState( ) { return state.toString( ); }
  public String getStateHint( ) { return stateHint; }
  public int getYarnMemory( ) { return yarnMemory; }
  public int getYarnVcores( ) { return yarnVcores; }
  public int getDrillTotalMemory( ) { return totalDrillMemory; }
  public int getDrillTotalVcores( ) { return totalDrillVcores; }
  public int getYarnNodeCount( ) { return yarnNodeCount; }
  public int getTaskCount( ) { return taskCount; }
  public int getLiveCount( ) { return liveCount; }
  public int getTargetCount( ) { return targetCount; }
  public List<PoolModel> getPools( ) { return pools; }

  private static Map<ClusterControllerImpl.State,String> stateHints = makeStateHints( );

  @Override
  public void visit(ClusterController controller) {
    ClusterControllerImpl impl = (ClusterControllerImpl) controller;

    appRpt = impl.getYarn().getAppHostReport();

    state = impl.getState( );
    stateHint = stateHints.get( state );
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

  private static Map<State, String> makeStateHints() {
    Map<ClusterControllerImpl.State,String> hints = new HashMap<>( );
    // UI likely will never display the FAILED state.
    hints.put( ClusterControllerImpl.State.START, "AM is starting up." );
    hints.put( ClusterControllerImpl.State.LIVE, "AM is operating normally." );
    hints.put( ClusterControllerImpl.State.ENDING, "AM is shutting down." );
    // UI will never display the ENDED state.
    hints.put( ClusterControllerImpl.State.ENDED, "AM is about to exit." );
    // UI will never display the FAILED state.
    hints.put( ClusterControllerImpl.State.FAILED, "AM failed to start and is about to exit." );
    return hints;
  }

}
