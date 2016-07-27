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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.drill.yarn.core.ContainerRequestSpec;
import org.apache.drill.yarn.core.LaunchSpec;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * The scheduler describes the set of tasks to run. It provides the details
 * required to launch each task and optionally a specification of the containers
 * required to run the task.
 * <p>
 * Schedulers can manage batch task (which do their job and complete), or
 * persistent tasks (which run until terminated.)
 * <p>
 * The scheduler tracks task completion (for batch tasks) and task levels (for
 * persistent tasks.)
 */

public abstract class Scheduler {
  public static class TaskSpec {
    /**
     * Number of YARN vcores (virtual cores) and amount of memory (in MB) needed
     * by this task.
     */

    public ContainerRequestSpec containerSpec;

    /**
     * Description of of the task process, environment and so on.
     */

    public LaunchSpec launchSpec;

    public int maxRetries;

    public String name;
  }
  
  private static final Log LOG = LogFactory.getLog(Scheduler.class);
  private final String name;
  private final String type;
  protected TaskSpec taskSpec;
  protected int priority;
  protected int failCount;
  private TaskManager taskManager;
  protected SchedulerState state;
  protected boolean isTracked;

  public Scheduler(String type, String name) {
    this.type = type;
    this.name = name;
    taskManager = new TaskManager();
  }

  public void setTaskManager(TaskManager taskManager) {
    this.taskManager = taskManager;
  }
  
  /**
   * Register the state object that tracks tasks launched by this scheduler.
   *
   * @param state
   */

  public void registerState(SchedulerState state) {
    this.state = state;
  }
  /**
   * Controller-assigned priority for this scheduler. Used to differentiate
   * container requests by scheduler.
   *
   * @param priority
   */

  public void setPriority(int priority) {
    this.priority = priority;
    taskSpec.containerSpec.priority = priority;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }
  /**
   * Get the desired number of running tasks.
   *
   * @return
   */
  
  public abstract int getTarget();

  public TaskManager getTaskManager() {
    return taskManager;
  }


  /**
   * Increase (positive) or decrease (negative) the number of desired tasks by
   * the given amount.
   *
   * @param delta
   */

  public void change(int delta) {
    resize(getTarget() + delta);
  }


  /**
   * Set the number of desired tasks to the given level.
   *
   * @param level
   */
  public abstract void resize(int level);

  public abstract void completed(Task task);

  /**
   * Adjust the number of running tasks to better track the desired number.
   * Starts or stops tasks using the {@link SchedulerState} registered with
   * {@link #registerState(SchedulerState)}.
   */

  public abstract void adjust();

  /**
   * Return an estimate of progress given as a ratio of (work completed, total
   * work).
   *
   * @return
   */
  public abstract int[] getProgress();

  /**
   * If this is a batch scheduler, whether all tasks for the batch have
   * completed. If this is a persistent task scheduler, always returns false.
   *
   * @return true if the scheduler has more tasks to run, false if the
   * scheduler has no more tasks or manages a set of long-running tasks
   */
  public abstract boolean hasMoreTasks();

  /**
   * For reporting, get the YARN resources requested by processes in
   * this pool.
   * @return
   */

  public ContainerRequestSpec getResource() {
    return taskSpec.containerSpec;
  }

  protected void addTasks(int n) {
    for (int i = 0; i < n; i++) {
      state.start(new Task(this, taskSpec));
    }
  }
  
  /**
   * Whether tasks from this scheduler should incorporate app startup/shutdown
   * acknowledgments (acks) into the task lifecycle.
   *
   * @return
   */

  public boolean isTracked() {
    return isTracked;
  }

  public void limitContainerSize(Resource maxResource) throws AMException {
    if (taskSpec.containerSpec.memoryMb > maxResource.getMemory()) {
      LOG.warn(taskSpec.name + " requires " + taskSpec.containerSpec.memoryMb
          + " MB but the maximum YARN container size is "
          + maxResource.getMemory() + " MB");
      taskSpec.containerSpec.memoryMb = maxResource.getMemory();
    }
    if (taskSpec.containerSpec.vCores > maxResource.getVirtualCores()) {
      LOG.warn(taskSpec.name + " requires " + taskSpec.containerSpec.vCores
          + " vcores but the maximum YARN container size is "
          + maxResource.getVirtualCores() + " vcores");
      taskSpec.containerSpec.vCores = maxResource.getVirtualCores();
    }
  }
}
