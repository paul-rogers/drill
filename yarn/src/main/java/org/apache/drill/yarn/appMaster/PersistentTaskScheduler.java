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

/**
 * Abstract base class for schedulers that work with persistent
 * (long-running) tasks. Such tasks are intended to run until
 * explicitly shut down (unlike batch tasks that run until
 * some expected completion.)
 * <p>
 * Provides a target quantity of tasks
 * (see {@link #getTarget()}, along with opeations to increase,
 * decrease or set the target number.
 * <p>
 * The scheduler acts as a controller: starting new tasks as needed to
 * match the desired target, or stopping tasks as needed when the
 * target level is reduced.
 */

public abstract class PersistentTaskScheduler extends AbstractScheduler
{
  protected int quantity;

  public PersistentTaskScheduler(String name, int quantity) {
    super(name);
    this.quantity = quantity;
  }

  /**
   * Set the number of running tasks to the quantity given.
   *
   * @param level the target number of tasks
   */

  @Override
  public void resize(int level) {
    quantity = level;
    if (quantity < 0) {
      quantity = 0; }
  }

  @Override
  public int getTarget() { return quantity; }

  /**
   * Indicate that a task is completed. Normally occurs only
   * when shutting down excess tasks.
   *
   * @param task
   */


  @Override
  public void completed(Task task) { }

  /**
   * Progress for persistent tasks defaults to the ratio of
   * running tasks to target level. Thus, a persistent cluster
   * will normally report 100% progress.
   *
   * @return
   */

  @Override
  public int[] getProgress() {
    int activeCount = state.getTaskCount();
    return new int[] { Math.min(activeCount, quantity), quantity };
  }

  /**
   * Adjust the number of running tasks to better match the target
   * by starting or stopping tasks as needed.
   */

  @Override
  public void adjust() {
    int activeCount = state.getTaskCount();
    int delta = quantity - activeCount;
    if (delta > 0) {
      addTasks(delta);
    } else if (delta < 0) {
      cancelTasks(activeCount); }
  }

  /**
   * Cancel the required number of tasks. We exclude any tasks that are already
   * in the process of being cancelled. Because we ignore those tasks, it might
   * be that we want to reduce the task count, but there is nothing to cancel,
   * since the required number of tasks have already been cancelled.
   *
   * @param activeCount
   */

  private void cancelTasks(int activeCount) {
    int cancelled = state.getCancelledTaskCount();
    int cancellable = activeCount - cancelled;
    int n = cancellable - quantity;
    if (n <= 0) {
      return; }
    for (Task task : state.getStartingTasks()) {
      state.cancel(task);
      if (--n == 0) {
        return; }
    }
    for (Task task : state.getActiveTasks()) {
      state.cancel(task);
      if (--n == 0) {
        return; }
    }

    // If we get here it means that we've already cancelled tasks
    // but they have not yet shut down.

    assert n <= cancelled;
  }

  /**
   * "Done" does not apply to persistent tasks. However, we will
   * claim we are done if we don't want to run any tasks at all.
   * @return
   */

  @Override
  public boolean isDone() {
    return quantity == 0;
  }
}
