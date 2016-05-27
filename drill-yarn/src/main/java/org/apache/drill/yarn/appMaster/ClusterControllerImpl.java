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

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.drill.yarn.appMaster.TaskLifecycleListener.Event;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;

/**
 * Controls the Drill cluster by representing the current cluster state with a
 * desired state, taking corrective action to keep the cluster in the desired
 * state. The cluster as a whole has a state, as do each task (node) within the
 * cluster.
 * <p>
 * This class is designed to allow unit tests. In general, testing the
 * controller on a live cluster is tedious. This class encapsulates the
 * controller algorithm so it can be driven by a simulated cluster.
 * <p>
 * This object is shared between threads, thus synchronized.
 */

public class ClusterControllerImpl implements ClusterController
{
  public enum State
  {
    START, LIVE, ENDING, ENDED, FAILED
  }

  private final static int PRIORITY_OFFSET = 1;

  private static final Log LOG = LogFactory.getLog(ClusterControllerImpl.class);

  /**
   * Signals the completion of the cluster run. The main program waits on this
   * mutex until all tasks complete (batch) or the cluster is explicitly shut
   * down (persistent tasks.)
   */

  private Object completionMutex = new Object();

  protected int maxRetries = 3;

  State state = State.START;

  /**
   * Definition of the task types that can be run by this controller, along with
   * the target task levels for each.
   */

  private Map<String, SchedulerStateActions> taskPools = new HashMap<>();

  private List<SchedulerStateActions> prioritizedPools = new ArrayList<>();

  private Set<ContainerId> allocatedContainers = new HashSet<>();

  /**
   * Cluster-wide list of active tasks. Allows lookup from container ID to task
   * (and then from task to task type.)
   */

  private Map<ContainerId, Task> activeContainers = new HashMap<>();

  /**
   * Tracks the tasks that have completed: either successfully (state == ENDED)
   * or failed (state == FAILED). Eventually store this information
   * elsewhere to avoid cluttering memory with historical data.
   *  Entries here are static copies, preserving the state
   * at the time that the task completed.
   */

  private List<Task> completedTasks = new LinkedList<>();

  private final AMYarnFacade yarn;

  private int maxRequestsPerTick = 2;

  private int stopTimoutMs = 10_000;

  private int configPollPeriod = 60_000;
  private long nextResourcePollTime;
  private int yarnNodeCount;

  private NodeInventory nodeInventory;

  private long lastFailureCheckTime;

  private int failureCheckPeriodMs = 60_000;

  private int taskCheckPeriodMs = 10_000;
  private long lastTaskCheckTime;

  private List<TaskLifecycleListener> lifecycleListeners = new ArrayList<>( );

   public ClusterControllerImpl(AMYarnFacade yarn) {
    this.yarn = yarn;
  }

  /**
   * Define a task type. Registration order is important: the controller starts
   * task in the order that they are registered. Must happen before the YARN
   * callbacks start.
   *
   * @param scheduler
   */

  @Override
  public void registerScheduler(Scheduler scheduler) {
    assert !taskPools.containsKey(scheduler.getName());
    scheduler.setPriority(taskPools.size() + PRIORITY_OFFSET);
    SchedulerStateActions taskGroup = new SchedulerStateImpl(this, scheduler);
    taskPools.put(taskGroup.getName(), taskGroup);
    prioritizedPools.add(taskGroup);
  }

  @Override
  public synchronized void started( ) throws YarnFacadeException, AMException
  {
    // Verify that no resource seeks a container larger than
    // what YARN can provide. Ensures a graceful exit in this
    // case.

    Resource maxResource = yarn.getRegistrationResponse().getMaximumResourceCapability();
    for (SchedulerStateActions group : prioritizedPools) {
      group.getScheduler().checkResources( maxResource );
    }
    nodeInventory = new NodeInventory( yarn );
    state = State.LIVE;
  }

  @Override
  public synchronized void tick(long curTime) {
    if ( state == State.LIVE ) {
      adjustTasks(curTime);
      requestContainers();
    }
    if ( state == State.LIVE  ||  state == State.ENDING ) {
      checkTasks(curTime);
    }
  }

  private void adjustTasks( long curTime ) {
    if ( nodeInventory.getFreeNodeCount() <= 0 ) {
      checkForFailure( curTime );
    }
    if ( state != State.LIVE ) {
      return; }
    for (SchedulerStateActions group : prioritizedPools) {
      group.adjustTasks();
    }
  }

  private void checkForFailure(long curTime) {
    if ( lastFailureCheckTime + failureCheckPeriodMs > curTime ) {
      return; }
    lastFailureCheckTime = curTime;
    for (SchedulerStateActions group : prioritizedPools) {
      if ( group.getTaskCount( ) > 0 ) {
        return; }
    }
    LOG.error("Application failure: no tasks are running and no nodes are available -- exiting.");
    terminate( State.FAILED );
  }

  private void checkTasks(long curTime) {
    if ( lastTaskCheckTime + taskCheckPeriodMs > curTime ) {
      return; }
    lastTaskCheckTime = curTime;
    EventContext context = new EventContext(this);
    for (SchedulerStateActions group : prioritizedPools) {
      context.setGroup(group);
      group.checkTasks( context, curTime );
    }
  }

  /**
   * Get an update from YARN on available resources.
   */

  @Override
  public void updateRMStatus()
  {
    long curTime = System.currentTimeMillis();
    if (nextResourcePollTime > curTime) {
      return; }

    yarnNodeCount = yarn.getNodeCount();
//    LOG.info("YARN reports " + yarnNodeCount + " nodes.");

//    Resource yarnResources = yarn.getResources();
//    if (yarnResources != null) {
//      LOG.info("YARN reports " + yarnResources.getMemory() + " MB, " + yarnResources.getVirtualCores()
//          + " vcores available.");
//    }
    nextResourcePollTime = curTime + configPollPeriod;
  }

  /**
   * Request any containers that have accumulated.
   */

  private void requestContainers() {
    EventContext context = new EventContext(this);
    for (SchedulerStateActions group : prioritizedPools) {
      context.setGroup(group);
      if ( group.requestContainers( context, maxRequestsPerTick ) ) {
        break; }
   }
  }

  @Override
  public synchronized void containersAllocated(List<Container> containers) {
    EventContext context = new EventContext(this);
    for (Container container : containers) {
      if (allocatedContainers.contains(container.getId())) {
        continue; }
      allocatedContainers.add(container.getId());
      int priority = container.getPriority().getPriority();
      int offset = priority - PRIORITY_OFFSET;
      if (offset < 0 || offset > prioritizedPools.size()) {
        LOG.error("Container allocated with unknown priority " + priority);
        continue;
      }
      context.setGroup(prioritizedPools.get(offset));
      context.group.containerAllocated(context, container);
    }
  }

  @Override
  public synchronized void containerStarted(ContainerId containerId) {
    Task task = getTask(containerId);
    if (task == null) {
      return; }
    EventContext context = new EventContext(this, task);
    context.getState().containerStarted(context);
    LOG.trace("Container started: " + containerId);
  }

  @Override
  public synchronized void taskStartFailed(ContainerId containerId, Throwable t) {
    Task task = getTask(containerId);
    if (task == null) {
      return; }
    EventContext context = new EventContext(this, task);
    context.getState().launchFailed(context, t);
  }

  private Task getTask(ContainerId containerId) {
    Task task = activeContainers.get(containerId);
    if (task == null) {
      LOG.error("No container state for " + containerId);
    }
    return task;
  }

  @Override
  public synchronized void containerStopped(ContainerId containerId) {
    // Ignored because the node manager notification is very
    // unreliable. Better to rely on the Resource Manager
    // completion request.
//    Task task = getTask(containerId);
//    if (task == null) {
//      return; }
//    EventContext context = new EventContext(this, task);
//    context.getState().containerStopped(context);
  }

  @Override
  public synchronized void containersCompleted(List<ContainerStatus> statuses) {
    EventContext context = new EventContext(this);
    for (ContainerStatus status : statuses) {
      Task task = getTask(status.getContainerId());
      if (task == null) {
        continue; }
      context.setTask(task);
      context.getState().containerCompleted(context, status);
    }
    checkStatus( );
  }

  @Override
  public synchronized float getProgress() {
    int numerator = 0;
    int denominator = 0;
    for (SchedulerStateActions group : taskPools.values()) {
      Scheduler sched = group.getScheduler();
      int[] progress = sched.getProgress();
      numerator += progress[0];
      denominator += progress[1];
    }
    if (numerator == 0) {
      return 1; }
    return (float) denominator / (float) numerator;
  }

  @Override
  public synchronized void stopTaskFailed(ContainerId containerId, Throwable t) {
    Task task = getTask(containerId);
    if (task == null) {
      return; }
    EventContext context = new EventContext(this, task);
    context.getState().stopTaskFailed(context, t);
  }

  @Override
  public synchronized void resizeDelta(int delta) {
    // TODO: offer the delta to each scheduler in turn.
    // For now, we support only one scheduler.

    prioritizedPools.get(0).getScheduler().change(delta);
  }

  @Override
  public synchronized void resizeTo(int n) {
    // TODO: offer the delta to each scheduler in turn.
    // For now, we support only one scheduler.

    prioritizedPools.get(0).getScheduler().resize(n);
  }

  @Override
  public synchronized void shutDown() {
    LOG.info("Shut down request received");
    this.state = State.ENDING;
    EventContext context = new EventContext(this);
    for (SchedulerStateActions group : prioritizedPools) {
      group.shutDown(context);
    }
    checkStatus( );
  }

  @Override
  public boolean waitForCompletion() {
    start();
    synchronized (completionMutex) {
      try {
        completionMutex.wait();
        LOG.info("Controller shut down completed");
      } catch (InterruptedException e) {
        // Should not happen
      }
    }
    return succeeded( );
  }

  private void start() {
    yarnReport();
  }

  private void yarnReport() {
    RegisterApplicationMasterResponse response = yarn.getRegistrationResponse();
    LOG.info("YARN queue: " + response.getQueue());
    Resource resource = response.getMaximumResourceCapability();
    LOG.info("YARN max resource: " + resource.getMemory() + " MB, " + resource.getVirtualCores() + " cores");
    EnumSet<SchedulerResourceTypes> types = response.getSchedulerResourceTypes();
    StringBuilder buf = new StringBuilder();
    String sep = "";
    for (SchedulerResourceTypes type : types) {
      buf.append(sep);
      buf.append(type.toString());
      sep = ", ";
    }
    LOG.info("YARN scheduler resource types: " + buf.toString());
  }

  /**
   * Check for overall completion. We are done when either we've successfully
   * run all tasks, or we've run some and given up on others. We're done when
   * the number of completed or failed tasks reaches our target.
   */

  private void checkStatus() {
    if ( state != State.ENDING ) {
      return; }
    for (SchedulerStateActions group : prioritizedPools) {
      if (!group.isDone()) {
        return; }
    }
    terminate( State.ENDED );
  }

  private void terminate(State state ) {
    this.state = state;
    synchronized (completionMutex) {
      completionMutex.notify();
    }
  }

  public boolean isLive() {
    return state == State.LIVE;
  }

  public boolean succeeded( ) {
    return state == State.ENDED;
  }

  public void containerAllocated(Task task) {
    activeContainers.put(task.getContainerId(), task);
  }

  public AMYarnFacade getYarn() { return yarn; }

  public void containerReleased(Task task) {
    activeContainers.remove(task.getContainerId());
  }

  public void taskEnded(Task task) {
    completedTasks.add(task);
  }

  public void taskRetried(Task task) {
    Task copy = task.copy( );
    copy.disposition = Task.Disposition.RETRIED;
    completedTasks.add(copy);
  }

  public void taskGroupCompleted(SchedulerStateActions taskGroup) {
    checkStatus();
  }

  public int getMaxRetries() { return maxRetries; }

  public int getStopTimeoutMs() { return stopTimoutMs; }

  @Override
  public synchronized void reserveHost(String hostName) {
    nodeInventory.reserve(hostName);
  }

  @Override
  public synchronized void releaseHost(String hostName) {
    nodeInventory.release(hostName);
  }

  public NodeInventory getNodeInventory() {return nodeInventory;}

  @Override
  public void registerLifecycleListener( TaskLifecycleListener listener ) {
    lifecycleListeners.add( listener );
  }

  public void fireLifecycleChange(Event event, EventContext context) {
    for ( TaskLifecycleListener listener : lifecycleListeners ) {
      listener.stateChange( event, context);
    }
  }

  @Override
  public void setMaxRetries(int value) {
    maxRetries = value;
  }

  @Override
  public int getTargetCount() {
    // TODO: Handle multiple pools
    return prioritizedPools.get( 0 ).getScheduler().getTarget();
  }

  public State getState() { return state; }

  @Override
  public synchronized void visit(ControllerVisitor visitor) {
    visitor.visit( this );
  }

  public List<SchedulerStateActions> getPools() {
    return prioritizedPools;
  }

  @Override
  public synchronized void visitTasks(TaskVisitor visitor) {
    for ( SchedulerStateActions pool : prioritizedPools ) {
      pool.visitTaskModels( visitor );
    }
  }

  public List<Task> getHistory() { return completedTasks; }

  @Override
  public boolean cancelTask(int id) {
    for (SchedulerStateActions group : prioritizedPools) {
      Task task = group.getTask( id );
      if ( task != null ) {
        group.cancel( task );
        return true;
      }
    }
    return false;
  }
}