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
package org.apache.drill.exec.work.foreman.rm;

import org.apache.drill.exec.proto.UserBitShared.QueryId;

/**
 * Default null query queue used when no other queue is configured.
 * This is really no queue at all: it always reports itself as
 * disabled, and if called anyway, allows all queries to proceed.
 * <p>
 * The purpose of this class is simply to allow cleaner code in
 * the Foreman: no special casing is needed to handle the queued
 * vs. unqueued cases.
 */

public class NullQueryQueue implements QueryQueue {

  @Override
  public boolean enabled() {
    return false;
  }

  @Override
  public QueueLease queue(QueryId queryId, double cost)
      throws QueueTimeoutException, QueryQueueException {
    return null;
  }

  @Override
  public void release(QueueLease lease) {
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

}
