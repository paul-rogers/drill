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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

/**
 * Dispatches YARN, timer and ZooKeeper events to the cluster controller.
 * Allows the controller to be independent of the plumbing needed to
 * receive events. Divides work among
 * various components to separate concerns. Three streams of events
 * feed into an app master "strategy". The three streams are
 * <ol>
 * <li>Resource manager</li>
 * <li>Node manager</li>
 * <li>Timer</li>
 * </ol>
 * <p>
 * This class is "lightly" multi-threaded: it responds to events
 * from the RM, NM and timer threads. Within each of these, events
 * are sequential. So, synchronization is needed across the three event
 * types, but not within event types. (That is, we won't see two RM events,
 * say, occurring at the same time from separate threads.)
 */

public class Dispatcher
{
  private static final Log LOG = LogFactory.getLog(Dispatcher.class);

  /**
   * Handle YARN Resource Manager events. This is a separate class to clarify
   * which events are from the Resource Manager.
   */

  private class ResourceCallback implements AMRMClientAsync.CallbackHandler
  {
    @Override
    public void onContainersAllocated(List<Container> containers) {
      controller.containersAllocated(containers);
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
      controller.containersCompleted(statuses);
    }

    @Override
    public void onShutdownRequest() {
      LOG.trace("Shutdown request");
      controller.shutDown();
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {
      LOG.trace("Nodes updated");
    }

    @Override
    public float getProgress() {
      // getProgress is called on each fetch from the NM response queue.
      // This is a good time to update status, even if it looks a bit
      // bizarre...

      controller.updateRMStatus( );
      return controller.getProgress();
    }

    @Override
    public void onError(Throwable e) {
      LOG.error("RM Error: " + e.getMessage());
    }
  }

  /**
   * Handle YARN Node Manager events. This is a separate class to clarify which
   * events are, in fact, from the node manager.
   */

  public class NodeCallback implements NMClientAsync.CallbackHandler
  {
    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
      controller.taskStartFailed(containerId, t);
    }

    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
      controller.containerStarted(containerId);
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
      LOG.trace("Container status: " + containerId + " - " + containerStatus.toString());
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
      LOG.trace("Container error: " + containerId + " - " + t.getMessage());
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
      controller.stopTaskFailed(containerId, t);
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
      controller.containerStopped(containerId);
    }
  }

  /**
   * Handle timer events: a constant tick to handle time-based actions such as
   * timeouts.
   */

  public class TimerCallback implements PulseRunnable.PulseCallback
  {
    /**
     * The lifecycle of each task is driven by RM and NM callbacks. We use the
     * timer to start the process. While this is overkill here, in a real app,
     * we'd check requested resource levels (which might change) and number of
     * tasks (which might change if tasks die), and take corrective action:
     * adding or removing tasks.
     */

    @Override
    public void onTick(long curTime) {
      for (Pollable pollable : pollables) {
        pollable.tick(curTime);
      }
      controller.tick(curTime);
    }
  }

  private AMYarnFacade yarn;
  private ClusterController controller;

  /**
   * Add-on tools that are called once on each timer tick.
   */

  private List<Pollable> pollables = new ArrayList<>();

  /**
   * Add-ons for which the dispatcher should managed the start/end lifecycle.
   */

  private List<DispatcherAddOn> addOns = new ArrayList<>();

  public void setYarn(AMYarnFacade yarn) throws YarnFacadeException {
    this.yarn = yarn;
    controller = new ClusterControllerImpl(yarn);
  }

  public ClusterController getController() { return controller; }

  public void registerPollable(Pollable pollable) {
    pollables.add(pollable);
  }

  public void registerAddOn(DispatcherAddOn addOn) {
    addOns.add(addOn);
  }

  public void run() throws YarnFacadeException {
    LOG.trace("Starting");
    setup();
    LOG.trace("Running");
    boolean success = controller.waitForCompletion();
    LOG.trace("Finishing");
    finish(success);
  }

  private void setup() throws YarnFacadeException {
    yarn.start(new ResourceCallback(), new NodeCallback(), new TimerCallback());
    yarn.register();
    controller.started();

    for (DispatcherAddOn addOn : addOns) {
      addOn.start(controller);
    }
  }

  private void finish(boolean success) throws YarnFacadeException {
    for (DispatcherAddOn addOn : addOns) {
      addOn.finish(controller);
    }

    yarn.finish(success);
  }
}