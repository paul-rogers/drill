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

import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.proto.UserProtos.QueryResultsMode;
import org.apache.drill.exec.proto.UserProtos.RunQuery;

public class QueryRunner {
  private final InternalClient client;
  private final RestQueryRequest request;
  private QueryId queryId;

  public QueryRunner(InternalClient client, RestQueryRequest request) {
    this.client = client;
    this.request = request;
  }

  public void runOne(String stmt) {
    final RunQuery runQuery = RunQuery.newBuilder()
        .setType(QueryType.SQL) // TODO: Declare a constant
        .setPlan(stmt)
        .setResultsMode(QueryResultsMode.STREAM_FULL)
        .setAutolimitRowcount(request.rowLimit())
        .build();
//    queryId = client.submitQuery(listener, runQuery);
  }
}
