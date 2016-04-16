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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Clock driver that calls a callback once each pulse period. Used to react to
 * time-based events such as timeouts, checking for changed files, etc.
 */

public class PulseRunnable implements Runnable
{
  /**
   * Interface implemented to receive calls on each clock "tick."
   */

  public interface PulseCallback
  {
    void onTick(long curTime);
  }

  private final int pulsePeriod;
  private final PulseRunnable.PulseCallback callback;
  public AtomicBoolean isLive = new AtomicBoolean(true);

  public PulseRunnable(int pulsePeriodMS, PulseRunnable.PulseCallback callback) {
    pulsePeriod = pulsePeriodMS;
    this.callback = callback;
  }

  @Override
  public void run() {
    while (isLive.get()) {
      try {
        Thread.sleep(pulsePeriod);
      } catch (InterruptedException e) {
        break;
      }
      callback.onTick(System.currentTimeMillis());
    }
  }

  public void stop() { isLive.set(false); }
}