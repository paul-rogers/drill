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

public abstract class AbstractScheduler implements Scheduler
{
  private final String name;
  protected TaskSpec taskSpec;
//  private SchedulerListener listener;
  protected int priority;
  protected int failCount;
  private TaskManager taskManager;
  protected SchedulerState state;
  protected boolean isTracked;

  public AbstractScheduler(String name) {
    this.name = name;
    taskManager = new AbstractTaskManager();
  }

  public void setTaskManager(TaskManager taskManager) {
    this.taskManager = taskManager;
  }

  @Override
  public void registerState(SchedulerState state) {
    this.state = state;
  }

  @Override
  public void setPriority(int priority) {
    this.priority = priority;
    taskSpec.containerSpec.priority = priority;
  }

  @Override
  public String getName() { return name; }

  @Override
  public TaskManager getTaskManager() { return taskManager; }

//  @Override
//  public void setListener(SchedulerListener listener) { this.listener = listener; }

  @Override
  public void change(int delta) { resize(getTarget() + delta); }

  protected void addTasks(int n) {
    for (int i = 0; i < n; i++) {
      state.start(new Task(this, taskSpec));
    }
  }

  @Override
  public boolean isTracked() { return isTracked; }
}
