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
import org.apache.drill.yarn.appMaster.AMYarnFacade.YarnFacadeException;
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

public class ClusterController {
  public interface TaskVisitor {
    void visit(Task task);
  }
  public interface ControllerVisitor {
    void visit(ClusterController controller);
  }
  /**
   * Controller lifecycle state.
   */

  public enum State {
    /**
     * Cluster is starting. Things are in a partially-built state. No tasks are
     * started until the cluster moves to LIVE.
     */

    START,

    /**
     * Normal operating state: the controller seeks to maintain the desired
     * number of tasks.
     */

    LIVE,

    /**
     * Controller is shutting down. Tasks are gracefully (where possible) ended;
     * no new tasks are started. (That is, when we detect the exit of a task,
     * the controller no longer immediately tries to start a replacement.
     */

    ENDING,

    /**
     * The controller has shut down. All tasks and threads are stopped. The
     * controller allows the main thread (which has been patiently waiting) to
     * continue, allowing the AM itself to shut down. Thus, this is a very
     * short-lived state.
     */

    ENDED,

    /**
     * Something bad happened on start-up; the AM can't start and must shut
     * down.
     */

    FAILED
  }

  private final static int PRIORITY_OFFSET = 1;

  private static final Log LOG = LogFactory.getLog(ClusterController.class);

  /**
   * Signals the completion of the cluster run. The main program waits on this
   * mutex until all tasks complete (batch) or the cluster is explicitly shut
   * down (persistent tasks.)
   */

  private Object completionMutex = new Object();

  /**
   * Maximum number of retries for each task launch.
   */

  protected int maxRetries = 3;

  /**
   * Controller state.
   *
   * @see {@link State}
   */

  State state = State.START;

  /**
   * Definition of the task types that can be run by this controller, along with
   * the target task levels for each.
   */

  private Map<String, SchedulerState> taskPools = new HashMap<>();

  /**
   * List of task pools prioritized in the order in which tasks should start.
   * DoY supports only one task pool at present. The idea is to, later, support
   * multiple pools that represent, say, pool 1 as the minimum number of
   * Drillbits to run at all times, with pool 2 as extra Drillbits to start up
   * during peak demand.
   * <p>
   * The priority also gives rise to YARN request priorities which are the only
   * tool the AM has to associate container grants with the requests to which
   * they correspond.
   */

  private List<SchedulerState> prioritizedGroups = new ArrayList<>();

  /**
   * Cluster-wide association of YARN container IDs to tasks.
   */

  private Set<ContainerId> allocatedContainers = new HashSet<>();

  /**
   * Cluster-wide list of active tasks. Allows lookup from container ID to task
   * (and then from task to task type.)
   */

  private Map<ContainerId, Task> activeContainers = new HashMap<>();

  /**
   * Tracks the tasks that have completed: either successfully (state == ENDED)
   * or failed (state == FAILED). Eventually store this information elsewhere to
   * avoid cluttering memory with historical data. Entries here are static
   * copies, preserving the state at the time that the task completed.
   */

  private List<Task> completedTasks = new LinkedList<>();

  /**
   * Wrapper around the YARN API. Abstracts the details of YARN operations.
   */

  private final AMYarnFacade yarn;

  /**
   * Maximum number of new tasks to start on each "pulse" tick.
   */

  private int maxRequestsPerTick = 2;

  private int stopTimoutMs = 10_000;

  /**
   * Time (in ms) between request to YARN to get an updated list of the node
   * "inventory".
   */

  private int configPollPeriod = 60_000;
  private long nextResourcePollTime;

  /**
   * List of nodes available in the cluster. Necessary as part of the process of
   * ensuring that we run one Drillbit per node. (The YARN blacklist only half
   * works for this purpose.)
   */

  private NodeInventory nodeInventory;

  private long lastFailureCheckTime;

  private int failureCheckPeriodMs = 60_000;

  private int taskCheckPeriodMs = 10_000;
  private long lastTaskCheckTime;

  /**
   * To increase code modularity, add-ons (such as the ZK monitor) register as
   * lifecycle listeners that are alerted to "interesting" lifecycle events.
   */

  private List<TaskLifecycleListener> lifecycleListeners = new ArrayList<>();

  /**
   * Handy mechanism for setting properties on this controller that are
   * available to plugins and UI without cluttering this class with member
   * variables.
   */

  private Map<String, Object> properties = new HashMap<>();

  /**
   * When enabled, allows the controller to check for failures that result in no
   * drillbits running. The controller will then automatically exit as no useful
   * work can be done. Disable this to make debugging easier on a single-node
   * cluster (lets you, say, start a "stray" drill bit and see what happens
   * without the AM exiting.)
   */

  private boolean enableFailureCheck = true;

  public ClusterController(AMYarnFacade yarn) {
    this.yarn = yarn;
  }

  public void enableFailureCheck(boolean flag) {
    this.enableFailureCheck = flag;
  }

  /**
   * Define a task type. Registration order is important: the controller starts
   * task in the order that they are registered. Must happen before the YARN
   * callbacks start.
   *
   * @param scheduler
   */

  public void registerScheduler(Scheduler scheduler) {
    assert !taskPools.containsKey(scheduler.getName());
    scheduler.setPriority(taskPools.size() + PRIORITY_OFFSET);
    SchedulerState taskGroup = new SchedulerState(this, scheduler);
    taskPools.put(taskGroup.getName(), taskGroup);
    prioritizedGroups.add(taskGroup);
  }

  /**
   * Called after the dispatcher has started YARN and other server
   * components. The controller can now begin to spin up tasks.
   */

  public synchronized void started() throws YarnFacadeException, AMException {
    nodeInventory = new NodeInventory(yarn);

    // Verify that no resource seeks a container larger than
    // what YARN can provide. Ensures a graceful exit in this
    // case.

    Resource maxResource = yarn.getRegistrationResponse()
        .getMaximumResourceCapability();
    for (SchedulerState group : prioritizedGroups) {
      group.getScheduler().limitContainerSize(maxResource);
    }
    state = State.LIVE;
  }

  /**
   * Called by the timer ("pulse") thread to trigger time-based events.
   *
   * @param curTime
   */

  public synchronized void tick(long curTime) {
    if (state == State.LIVE) {
      adjustTasks(curTime);
      requestContainers();
    }
    if (state == State.LIVE || state == State.ENDING) {
      checkTasks(curTime);
    }
  }

  /**
   * Adjust the number of running tasks to match the desired level.
   *
   * @param curTime
   */

  private void adjustTasks(long curTime) {
    if (enableFailureCheck && nodeInventory.getFreeNodeCount() <= 0) {
      checkForFailure(curTime);
    }
    if (state != State.LIVE) {
      return;
    }
    for (SchedulerState group : prioritizedGroups) {
      group.adjustTasks();
    }
  }

  /**
   * Check if the controller is unable to run any tasks. If so, and the option
   * is enabled, then automatically exit since no useful work can be done.
   *
   * @param curTime
   */

  private void checkForFailure(long curTime) {
    if (lastFailureCheckTime + failureCheckPeriodMs > curTime) {
      return;
    }
    lastFailureCheckTime = curTime;
    for (SchedulerState group : prioritizedGroups) {
      if (group.getTaskCount() > 0) {
        return;
      }
    }
    LOG.error(
        "Application failure: no tasks are running and no nodes are available -- exiting.");
    terminate(State.FAILED);
  }

  /**
   * Periodically check tasks, handling any timeout issues.
   *
   * @param curTime
   */

  private void checkTasks(long curTime) {

    // Check periodically, not on every tick.

    if (lastTaskCheckTime + taskCheckPeriodMs > curTime) {
      return;
    }
    lastTaskCheckTime = curTime;

    // Check for task timeouts in states that have a timeout.

    EventContext context = new EventContext(this);
    for (SchedulerState group : prioritizedGroups) {
      context.setGroup(group);
      group.checkTasks(context, curTime);
    }
  }

  /**
   * Get an update from YARN on available resources.
   */

  public void updateRMStatus() {
    long curTime = System.currentTimeMillis();
    if (nextResourcePollTime > curTime) {
      return;
    }

    // yarnNodeCount = yarn.getNodeCount();
    // LOG.info("YARN reports " + yarnNodeCount + " nodes.");

    // Resource yarnResources = yarn.getResources();
    // if (yarnResources != null) {
    // LOG.info("YARN reports " + yarnResources.getMemory() + " MB, " +
    // yarnResources.getVirtualCores()
    // + " vcores available.");
    // }
    nextResourcePollTime = curTime + configPollPeriod;
  }

  /**
   * Request any containers that have accumulated.
   */

  private void requestContainers() {
    EventContext context = new EventContext(this);
    for (SchedulerState group : prioritizedGroups) {
      context.setGroup(group);
      if (group.requestContainers(context, maxRequestsPerTick)) {
        break;
      }
    }
  }

  /**
   * The RM has allocated one or more containers in response to container
   * requests submitted to the RM.
   *
   * @param containers
   *          the set of containers provided by YARN
   * @return the set of tasks to launch
   */

  public synchronized void containersAllocated(List<Container> containers) {
    EventContext context = new EventContext(this);
    for (Container container : containers) {
      if (allocatedContainers.contains(container.getId())) {
        continue;
      }
      allocatedContainers.add(container.getId());
      int priority = container.getPriority().getPriority();
      int offset = priority - PRIORITY_OFFSET;
      if (offset < 0 || offset > prioritizedGroups.size()) {
        LOG.error("Container allocated with unknown priority " + priority);
        continue;
      }
      context.setGroup(prioritizedGroups.get(offset));
      context.group.containerAllocated(context, container);
    }
  }

  /**
   * The NM reports that a container has successfully started.
   *
   * @param containerId
   *          the container which started
   */

  public synchronized void containerStarted(ContainerId containerId) {
    Task task = getTask(containerId);
    if (task == null) {
      return;
    }
    EventContext context = new EventContext(this, task);
    context.getState().containerStarted(context);
    LOG.trace("Container started: " + containerId);
  }

  /**
   * The RM API reports that an attempt to start a container has failed locally.
   *
   * @param containerId
   *          the container that failed to launch
   * @param t
   *          the error that occurred
   */

  public synchronized void taskStartFailed(ContainerId containerId,
      Throwable t) {
    Task task = getTask(containerId);
    if (task == null) {
      return;
    }
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

  /**
   * The Node Manager reports that a container has stopped.
   *
   * @param containerId
   */

  public synchronized void containerStopped(ContainerId containerId) {
    // Ignored because the node manager notification is very
    // unreliable. Better to rely on the Resource Manager
    // completion request.
    // Task task = getTask(containerId);
    // if (task == null) {
    // return; }
    // EventContext context = new EventContext(this, task);
    // context.getState().containerStopped(context);
  }

  /**
   * The Resource Manager reports that containers have completed with the given
   * statuses. Find the task for each container and mark them as completed.
   *
   * @param statuses
   */

  public synchronized void containersCompleted(List<ContainerStatus> statuses) {
    EventContext context = new EventContext(this);
    for (ContainerStatus status : statuses) {
      Task task = getTask(status.getContainerId());
      if (task == null) {
        continue;
      }
      context.setTask(task);
      context.getState().containerCompleted(context, status);
    }
    checkStatus();
  }

  public synchronized float getProgress() {
    int numerator = 0;
    int denominator = 0;
    for (SchedulerState group : taskPools.values()) {
      Scheduler sched = group.getScheduler();
      int[] progress = sched.getProgress();
      numerator += progress[0];
      denominator += progress[1];
    }
    if (numerator == 0) {
      return 1;
    }
    return (float) denominator / (float) numerator;
  }

  /**
   * The Node Manager API reports that a request sent to the NM to stop a task
   * has failed.
   *
   * @param containerId
   *          the container that failed to stop
   * @param t
   *          the reason that the stop request failed
   */

  public synchronized void stopTaskFailed(ContainerId containerId,
      Throwable t) {
    Task task = getTask(containerId);
    if (task == null) {
      return;
    }
    EventContext context = new EventContext(this, task);
    context.getState().stopTaskFailed(context, t);
  }

  /**
   * Request to resize the Drill cluster by a relative amount.
   *
   * @param delta
   *          the amount of change. Can be positive (to grow) or negative (to
   *          shrink the cluster)
   */

  public synchronized void resizeDelta(int delta) {
    // TODO: offer the delta to each scheduler in turn.
    // For now, we support only one scheduler.

    prioritizedGroups.get(0).getScheduler().change(delta);
  }

  /**
   * Request to resize the Drill cluster to the given size.
   *
   * @param n
   *          the desired cluster size
   */

  public synchronized void resizeTo(int n) {
    // TODO: offer the delta to each scheduler in turn.
    // For now, we support only one scheduler.

    prioritizedGroups.get(0).getScheduler().resize(n);
  }

  /**
   * Indicates a request to gracefully shut down the cluster.
   */

  public synchronized void shutDown() {
    LOG.info("Shut down request received");
    this.state = State.ENDING;
    EventContext context = new EventContext(this);
    for (SchedulerState group : prioritizedGroups) {
      group.shutDown(context);
    }
    checkStatus();
  }

  /**
   * Called by the main thread to wait for the normal shutdown of the
   * controller. Such shutdown occurs when the admin sends a sutdown
   * command from the UI or REST API.
   *
   * @return
   */

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
    return succeeded();
  }

  private void start() {
    yarnReport();
  }

  private void yarnReport() {
    RegisterApplicationMasterResponse response = yarn.getRegistrationResponse();
    LOG.info("YARN queue: " + response.getQueue());
    Resource resource = response.getMaximumResourceCapability();
    LOG.info("YARN max resource: " + resource.getMemory() + " MB, "
        + resource.getVirtualCores() + " cores");
    EnumSet<SchedulerResourceTypes> types = response
        .getSchedulerResourceTypes();
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
    if (state != State.ENDING) {
      return;
    }
    for (SchedulerState group : prioritizedGroups) {
      if (!group.isDone()) {
        return;
      }
    }
    terminate(State.ENDED);
  }

  private void terminate(State state) {
    this.state = state;
    synchronized (completionMutex) {
      completionMutex.notify();
    }
  }

  public boolean isLive() {
    return state == State.LIVE;
  }

  public boolean succeeded() {
    return state == State.ENDED;
  }

  public void containerAllocated(Task task) {
    activeContainers.put(task.getContainerId(), task);
  }

  public AMYarnFacade getYarn() {
    return yarn;
  }

  public void containerReleased(Task task) {
    activeContainers.remove(task.getContainerId());
  }

  public void taskEnded(Task task) {
    completedTasks.add(task);
  }

  public void taskRetried(Task task) {
    Task copy = task.copy();
    copy.disposition = Task.Disposition.RETRIED;
    completedTasks.add(copy);
  }

  public void taskGroupCompleted(SchedulerState taskGroup) {
    checkStatus();
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public int getStopTimeoutMs() {
    return stopTimoutMs;
  }

  public synchronized void reserveHost(String hostName) {
    nodeInventory.reserve(hostName);
  }

  public synchronized void releaseHost(String hostName) {
    nodeInventory.release(hostName);
  }

  public NodeInventory getNodeInventory() {
    return nodeInventory;
  }

  public void setProperty(String key, Object value) {
    properties.put(key, value);
  }

  public Object getProperty(String key) {
    return properties.get(key);
  }

  public void registerLifecycleListener(TaskLifecycleListener listener) {
    lifecycleListeners.add(listener);
  }

  public void fireLifecycleChange(Event event, EventContext context) {
    for (TaskLifecycleListener listener : lifecycleListeners) {
      listener.stateChange(event, context);
    }
  }

  public void setMaxRetries(int value) {
    maxRetries = value;
  }

  /**
   * Return the target number of tasks that the controller seeks to maintain.
   * This is the sum across all pools.
   *
   * @return
   */

  public int getTargetCount() {
    int count = 0;
    for (SchedulerState group : prioritizedGroups) {
      count += group.getScheduler().getTarget();
    }
    return count;
  }

  public State getState() {
    return state;
  }

  /**
   * Allow an observer to see a consistent view of the controller's
   * state by performing the visit in a synchronized block.
   * @param visitor
   */

  public synchronized void visit(ControllerVisitor visitor) {
    visitor.visit(this);
  }

  public List<SchedulerState> getSchedulers() {
    return prioritizedGroups;
  }

  /**
   * Allow an observer to see a consistent view of the controller's
   * task state by performing the visit in a synchronized block.
   *
   * @param visitor
   */

  public synchronized void visitTasks(TaskVisitor visitor) {
    for (SchedulerState pool : prioritizedGroups) {
      pool.visitTaskModels(visitor);
    }
  }

  public List<Task> getHistory() {
    return completedTasks;
  }

  /**
   * Cancels the given task, reducing the target task count. Called
   * from the UI to allow the user to select the specific task to end
   * when reducing cluster size.
   *
   * @param id
   * @return
   */

  public synchronized boolean cancelTask(int id) {
    for (SchedulerState group : prioritizedGroups) {
      Task task = group.getTask(id);
      if (task != null) {
        group.cancel(task);
        group.getScheduler().change(-1);
      }
    }
    return false;
  }

  public synchronized void completionAck(Task task, String propertyKey) {
    EventContext context = new EventContext(this);
    context.setTask(task);
    context.getState().completionAck(context);
    if (propertyKey != null) {
      task.properties.remove(propertyKey);
    }
  }

  public synchronized void startAck(Task task, String propertyKey,
      Object value) {
    if (propertyKey != null && value != null) {
      task.properties.put(propertyKey, value);
    }
    EventContext context = new EventContext(this);
    context.setTask(task);
    context.getState().startAck(context);
  }

  /**
   * Whether this distribution of YARN supports disk resources.
   * @return
   */

  public boolean supportsDiskResource() {
    return getYarn().supportsDiskResource();
  }

  public void registryDown() { shutDown( ); }
}