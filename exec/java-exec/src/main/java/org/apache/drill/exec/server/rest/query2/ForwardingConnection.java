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
package org.apache.drill.exec.server.rest.query2;

import java.util.concurrent.BlockingQueue;

import org.apache.drill.exec.physical.impl.materialize.QueryDataPackage;
import org.apache.drill.exec.physical.impl.materialize.QueryWritableBatch;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.server.rest.BaseWebUserConnection;
import org.apache.drill.exec.server.rest.WebUserConnection;
import org.apache.drill.shaded.guava.com.google.common.collect.Queues;

public class ForwardingConnection extends BaseWebUserConnection {

  private final BlockingQueue<QueryWritableBatch> queue = Queues.newLinkedBlockingQueue();

  public ForwardingConnection(WebUserConnection webConn) {
    super(webConn.resources());
  }

  @Override
  public void sendData(RpcOutcomeListener<Ack> listener,
      QueryDataPackage data) {
    try {
      queue.put(data.toWritableBatch());
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
