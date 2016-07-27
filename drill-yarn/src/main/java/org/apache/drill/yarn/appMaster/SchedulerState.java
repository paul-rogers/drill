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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.drill.yarn.appMaster.ClusterController.TaskVisitor;
import org.apache.drill.yarn.appMaster.Task.TaskLifecycleListener;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;

/**
 * The cluster state for tasks managed by a scheduler. Abstracts away the
 * details of managing tasks, allowing the scheduler to work only with overall
 * number of tasks.
 * <p>
 * Manages a the set of tasks associated with a scheduler. The scheduler decides
 * which tasks to run or stop; the task group manages the life-cycle of the
 * tasks for the given scheduler.
 * <p>
 * Schedulers, and hence their groups, define a priority. When starting, higher
 * priority (lower priority value) groups run before lower priority groups.
 * Similarly, when shrinking the cluster, lower priority groups shrink before
 * higher priority groups.
 */

public final class SchedulerState {
  static final Log LOG = LogFactory.getLog(SchedulerState.class);

  private final Scheduler scheduler;

  private final ClusterController controller;

  /**
   * Tracks the tasks to be started, but for which no work has yet been done.
   * (State == PENDING).
   */

  protected List<Task> pendingTasks = new LinkedList<>();

  /**
   * Tracks the tasks for which containers have been requested. (State ==
   * REQUESTED).
   */

  protected List<Task> allocatingTasks = new LinkedList<>();

  /**
   * Tracks running tasks: those that have been allocated containers and are
   * starting, running, failed or ended. We use a map for this because, during
   * these states, the task is identified by its container. (State == LAUNCHING,
   * RUNNING or ENDING).
   */

  protected Map<ContainerId, Task> activeContainers = new HashMap<>();

  public SchedulerState(ClusterController controller,
      Scheduler scheduler) {
    this.controller = controller;
    this.scheduler = scheduler;
    scheduler.registerState(this);
  }

  /**
   * Returns the name of the scheduler associated with this task action group.
   *
   * @return
   */

  public String getName() {
    return scheduler.getName();
  }

  public int getMaxRetries() {
    return controller.getMaxRetries();
  }

  public int getStopTimeoutMs() {
    return controller.getStopTimeoutMs();
  }

  /**
   * Returns the scheduler associated with this task group.
   *
   * @return
   */

  public Scheduler getScheduler() { return scheduler; }

  /**
   * Define a new task in this group. Adds it to the pending queue so that a
   * container will be requested.
   *
   * @param task
   */

  public void start(Task task) {
    assert task.getGroup() == null;
    task.setGroup(this);
    enqueuePendingRequest(task);
  }

  /**
   * Put a task into the queue waiting to send a container request to YARN.
   *
   * @param task
   */

  public void enqueuePendingRequest(Task task) {
    assert !activeContainers.containsValue(task);
    assert !allocatingTasks.contains(task);
    assert !pendingTasks.contains(task);
    pendingTasks.add(task);

    // Special initial-state notification

    EventContext context = new EventContext(controller, task);
    controller.fireLifecycleChange(TaskLifecycleListener.Event.CREATED,
        context);
  }

  public int maxCurrentRequests() {
    return this.scheduler.getTaskManager().maxConcurrentAllocs();
  }

  /**
   * Request a container the first task that we wish to start.
   */

  public boolean requestContainers(EventContext context, int maxRequests) {
    if (pendingTasks.isEmpty()) {
      return false;
    }

    maxRequests = Math.min(maxRequests, maxCurrentRequests());
    for (int i = 0; i < maxRequests && !pendingTasks.isEmpty(); i++) {
      context.setTask(pendingTasks.get(0));
      context.getState().requestContainer(context);
    }
    return true;
  }

  /**
   * Remove a task from the queue of tasks waiting to send a container request.
   * The caller must put the task into the proper next state: the allocating
   * queue or the completed task list.
   *
   * @param task
   */

  public void dequeuePendingRequest(Task task) {
    assert !activeContainers.containsValue(task);
    assert !allocatingTasks.contains(task);
    assert pendingTasks.contains(task);
    pendingTasks.remove(task);
  }

  /**
   * Put a task onto the queue awaiting an allocation response from YARN.
   *
   * @param task
   */

  public void enqueueAllocatingTask(Task task) {
    assert !activeContainers.containsValue(task);
    assert !allocatingTasks.contains(task);
    assert !pendingTasks.contains(task);
    allocatingTasks.add(task);
  }

  /**
   * A container request has been granted. Match the container up with the first
   * task waiting for a container and launch the task.
   *
   * @param context
   * @param container
   */

  public void containerAllocated(EventContext context, Container container) {
    if (activeContainers.containsKey(container.getId())) {
      LOG.error("Container allocated again: " + container.getId());
      return;
    }
    if (allocatingTasks.isEmpty()) {

      // Not sure why this happens. Maybe only in debug mode
      // due stopping execution one thread while the RM
      // heartbeat keeps sending our request over & over?

      LOG.error("Allocated more containers than there are allocating tasks");
      context.yarn.releaseContainer(container);
      return;
    }
    context.setTask(allocatingTasks.get(0));
    context.getState().containerAllocated(context, container);
  }

  public void checkTasks(EventContext context, long curTime) {
    for (Task task : allocatingTasks) {
      context.setTask(task);
      context.getState().tick(context, curTime);
    }
    for (Task task : pendingTasks) {
      context.setTask(task);
      context.getState().tick(context, curTime);
    }
    for (Task task : activeContainers.values()) {
      context.setTask(task);
      context.getState().tick(context, curTime);
    }
  }

  /**
   * Remove a task from the list of those waiting for a container allocation.
   * The allocation may be done, or cancelled. The caller is responsible for
   * moving the task to the next collection.
   *
   * @param task
   */

  public void dequeueAllocatingTask(Task task) {
    assert allocatingTasks.contains(task);
    allocatingTasks.remove(task);
  }

  /**
   * Mark that a task has become active and should be tracked by its container
   * ID. Prior to this, the task is not associated with a container.
   *
   * @param task
   */

  public void containerAllocated(Task task) {
    assert !activeContainers.containsValue(task);
    assert !allocatingTasks.contains(task);
    assert !pendingTasks.contains(task);
    activeContainers.put(task.getContainerId(), task);
    controller.containerAllocated(task);
  }

  /**
   * Mark that a task has completed: its container has expired or been revoked
   * or the task has completed: successfully or a failure, as given by the
   * task's disposition. The task can no longer be tracked by its container ID.
   * If this is the last active task for this group, mark the group itself as
   * completed.
   *
   * @param task
   */

  public void containerReleased(Task task) {
    assert activeContainers.containsKey(task.getContainerId());
    activeContainers.remove(task.getContainerId());
    controller.containerReleased(task);
  }

  /**
   * Mark that a task has completed successfully or a failure, as given by the
   * task's disposition. If this is the last active task for this group, mark
   * the group itself as completed.
   *
   * @param task
   */

  public void taskEnded(Task task) {
    scheduler.completed(task);
    controller.taskEnded(task);
    if (isDone()) {
      controller.taskGroupCompleted(this);
    }
    LOG.info("Task completed: " + task.toString());
  }

  /**
   * Mark that a task is about to be retried. Task still retains its state from
   * the current try.
   *
   * @param task
   */

  public void taskRetried(Task task) {
    controller.taskRetried(task);
  }

  /**
   * Shut down this task group by canceling all tasks not already cancelled.
   *
   * @param context
   */

  public void shutDown(EventContext context) {
    for (Task task : getStartingTasks()) {
      context.setTask(task);
      context.getState().cancel(context);
    }
    for (Task task : getActiveTasks()) {
      context.setTask(task);
      context.getState().cancel(context);
    }
  }

  /**
   * Report if this task group has any tasks in the active part of their
   * life-cycle: pending, allocating or active.
   *
   * @return
   */

  public boolean hasTasks() {
    return getTaskCount() != 0;
  }

  /**
   * Determine if this task group is done. It is done when there are no active
   * tasks and the controller itself is shutting down. This latter check
   * differentiates the start state (when no tasks are active) from the end
   * state. The AM will not shut down until all task groups are done.
   *
   * @return
   */

  public boolean isDone() {
    return !hasTasks() && !scheduler.hasMoreTasks();
  }

  /**
   * Adjust the number of running tasks as needed to balance the number of
   * running tasks with the desired number. May result in no change it the
   * cluster is already in balance (or is in the process of achieving balance.)
   */

  public void adjustTasks() {
    scheduler.adjust();
  }

  /**
   * Request a graceful stop of the task. Delegates to the task manager to do
   * the actual work.
   *
   * @return true if the graceful stop request was sent, false if not, or if
   *         this task type has no graceful stop
   */

  public boolean requestStop(Task task) {
    return scheduler.getTaskManager().stop(task);
  }

  /**
   * The number of tasks in any active (non-ended) lifecycle state.
   *
   * @return
   */

  public int getTaskCount() {
    return pendingTasks.size() + allocatingTasks.size()
        + activeContainers.size();
  }

  /**
   * The number of active tasks that have been cancelled, but have not yet
   * ended.
   *
   * @return
   */

  public int getCancelledTaskCount() {

    // TODO Crude first cut. This value should be maintained
    // as a count.

    int count = 0;
    for (Task task : pendingTasks) {
      if (task.isCancelled()) {
        count++;
      }
    }
    for (Task task : allocatingTasks) {
      if (task.isCancelled()) {
        count++;
      }
    }
    for (Task task : activeContainers.values()) {
      if (task.isCancelled()) {
        count++;
      }
    }
    return count;
  }

  /**
   * Returns the list of tasks awaiting a container request to be sent to YARN
   * or for which a container request has been sent to YARN, but no container
   * allocation has yet been received. Such tasks are simple to cancel. The list
   * does not contain any tasks in this state which have previously been
   * cancelled.
   *
   * @return
   */

  public List<Task> getStartingTasks() {
    List<Task> tasks = new ArrayList<>();
    for (Task task : pendingTasks) {
      if (!task.isCancelled()) {
        tasks.add(task);
      }
    }
    for (Task task : allocatingTasks) {
      if (!task.isCancelled()) {
        tasks.add(task);
      }
    }
    return tasks;
  }

  /**
   * Returns the list of active tasks that have not yet been cancelled. Active
   * tasks are any task for which a container has been assigned, but has not yet
   * received a RM container completion event.
   *
   * @return
   */

  public List<Task> getActiveTasks() {
    List<Task> tasks = new ArrayList<>();
    for (Task task : activeContainers.values()) {
      if (!task.isCancelled()) {
        tasks.add(task);
      }
    }
    return tasks;
  }

  public void cancel(Task task) {
    EventContext context = new EventContext(controller, task);
    context.getState().cancel(context);
  }

  public int getLiveCount() {
    int count = 0;
    for (Task task : activeContainers.values()) {
      if (task.isLive()) {
        count++;
      }
    }
    return count;
  }

  public void visitTaskModels(TaskVisitor visitor) {
    for (Task task : pendingTasks) {
      visitor.visit(task);
    }
    for (Task task : allocatingTasks) {
      visitor.visit(task);
    }
    for (Task task : activeContainers.values()) {
      visitor.visit(task);
    }
  }

  public Task getTask(int id) {
    for (Task task : pendingTasks) {
      if (task.getId() == id) {
        return task;
      }
    }
    for (Task task : allocatingTasks) {
      if (task.getId() == id) {
        return task;
      }
    }
    for (Task task : activeContainers.values()) {
      if (task.getId() == id) {
        return task;
      }
    }
    return null;
  }
}