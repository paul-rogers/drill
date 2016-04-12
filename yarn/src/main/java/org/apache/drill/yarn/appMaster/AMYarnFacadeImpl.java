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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.drill.yarn.appMaster.PulseRunnable.PulseCallback;
import org.apache.drill.yarn.core.LaunchSpec;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * Wrapper around the asynchronous versions of the YARN AM-RM and AM-NM
 * interfaces. Allows strategy code to factor out the YARN-specific bits so that
 * strategy code is simpler. Also allows replacing the actual YARN code with a
 * mock for unit testing.
 */

public class AMYarnFacadeImpl implements AMYarnFacade
{
  private static final Log LOG = LogFactory.getLog(AMYarnFacadeImpl.class);

  private YarnConfiguration conf;
  private AMRMClientAsync<ContainerRequest> resourceMgr;
  private NMClientAsync nodeMgr;
  private RegisterApplicationMasterResponse registration;
  private YarnClient client;
  private Thread pulseThread;
  private int pollPeriodMs;
  private int timerPeriodMs;
  private PulseRunnable timer;

  public AMYarnFacadeImpl(int pollPeriodMs, int timerPeriodMs) {
    this.pollPeriodMs = pollPeriodMs;
    this.timerPeriodMs = timerPeriodMs;
  }

  @Override
  public void start(CallbackHandler resourceCallback,
      org.apache.hadoop.yarn.client.api.async.NMClientAsync.CallbackHandler nodeCallback, PulseCallback timerCallback) {

    conf = new YarnConfiguration();

    resourceMgr = AMRMClientAsync.createAMRMClientAsync(pollPeriodMs, resourceCallback);
    resourceMgr.init(conf);
    resourceMgr.start();

    // Create the asynchronous node manager client

    nodeMgr = NMClientAsync.createNMClientAsync(nodeCallback);
    nodeMgr.init(conf);
    nodeMgr.start();

    client = YarnClient.createYarnClient();
    client.init(conf);
    client.start();

    timer = new PulseRunnable(timerPeriodMs, timerCallback);
  }

  @Override
  public void register() throws YarnFacadeException {
    String appMasterTrackingUrl = "";
    try {
      registration = resourceMgr.registerApplicationMaster("", 0, appMasterTrackingUrl);
    } catch (YarnException | IOException e) {
      throw new YarnFacadeException("Register AM failed", e);
    }

    // Start the pulse thread after registering so that we're in
    // a state where we can interact with the RM.

    pulseThread = new Thread(timer);
    pulseThread.setName("Pulse");
    pulseThread.start();
  }

  @Override
  public ContainerRequest requestContainer(ContainerRequestSpec containerSpec) {
    ContainerRequest request = containerSpec.makeRequest();
    resourceMgr.addContainerRequest(containerSpec.makeRequest());
    return request;
  }

  @Override
  public void launchContainer(Container container, LaunchSpec taskSpec) throws YarnFacadeException {
    ContainerLaunchContext context = createLaunchContext(taskSpec);
    startContainerAsync(container, context);
  }

  private ContainerLaunchContext createLaunchContext(LaunchSpec task) throws YarnFacadeException {
    try {
      return task.createLaunchContext(conf);
    } catch (IOException e) {
      throw new YarnFacadeException("Failed to create launch context", e);
    }
  }

  private void startContainerAsync(Container container, ContainerLaunchContext context) {
    nodeMgr.startContainerAsync(container, context);
  }

  @Override
  public void finish(boolean succeeded) throws YarnFacadeException
  {
    // Stop the timer thread first. This ensures that the
    // timer events don't try to use the YARN API during
    // shutdown.

    timer.stop();
    while (pulseThread.isAlive()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // Ignore;
      }
    }

    // Then the Node Manager client.

    nodeMgr.stop();

    // Deregister the app from YARN.

    String appMsg = "Drill Cluster Shut-Down";
    FinalApplicationStatus status = FinalApplicationStatus.SUCCEEDED;
    if ( ! succeeded ) {
      appMsg = "Drill Cluster Fatal Error - check logs";
      status = FinalApplicationStatus.FAILED;
    }
    try {
      resourceMgr.unregisterApplicationMaster(status, appMsg, "");
    } catch (YarnException | IOException e) {
      throw new YarnFacadeException("Deregister AM failed", e);
    }

    // Stop the Resource Manager client

    resourceMgr.stop();
  }

  @Override
  public void releaseContainer(Container container) {
    resourceMgr.releaseAssignedContainer(container.getId());
  }

  @Override
  public void killContainer(Container container) {
    nodeMgr.stopContainerAsync(container.getId(), container.getNodeId());
  }

  @Override
  public int getNodeCount() {
    return resourceMgr.getClusterNodeCount();
  }

  @Override
  public Resource getResources() {
    return resourceMgr.getAvailableResources();
  }

  @Override
  public void removeContainerRequest(ContainerRequest containerRequest) {
    resourceMgr.removeContainerRequest(containerRequest);
  }

  @Override
  public RegisterApplicationMasterResponse getRegistrationResponse() {
    return registration;
  }

  @Override
  public void blacklistNode(String nodeName) {
//    String nodeAddr = nodeMap.get( nodeName );
//    if ( nodeAddr == null )
//      nodeAddr = nodeName;
    resourceMgr.updateBlacklist(Collections.singletonList(nodeName), null);
  }

  @Override
  public void removeBlacklist(String nodeName) {
//    String nodeAddr = nodeMap.get( nodeName );
//    if ( nodeAddr == null )
//      nodeAddr = nodeName;
    resourceMgr.updateBlacklist(null, Collections.singletonList(nodeName));
  }

  @Override
  public List<NodeReport> getNodeReports() throws YarnFacadeException {
    try {
      return client.getNodeReports(NodeState.RUNNING);
    } catch (Exception e) {
      throw new YarnFacadeException( "getNodeReports failed" , e );
    }
  }
}