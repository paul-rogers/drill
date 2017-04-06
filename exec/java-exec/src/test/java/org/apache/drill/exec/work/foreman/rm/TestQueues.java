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
 ******************************************************************************/
package org.apache.drill.exec.work.foreman.rm;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.work.foreman.rm.EmbeddedQueryQueue;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.FixtureBuilder;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.apache.drill.test.QueryBuilder.QuerySummaryFuture;
import org.junit.Test;

public class TestQueues {

  @Test
  public void test() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(EmbeddedQueryQueue.ENABLED, true)
        .configProperty(EmbeddedQueryQueue.QUEUE_SIZE, 2)
        .configProperty(EmbeddedQueryQueue.TIMEOUT_MS, 5000)
        ;
    try(ClusterFixture cluster = builder.build();
        ClientFixture client = cluster.clientFixture()) {
      List<QuerySummaryFuture> futures = new ArrayList<>( );
      int n = 10;
      for( int i = 0; i < n; i++ ) {
        futures.add( client.queryBuilder().sql("SELECT `id_i` FROM `mock`.`implicit_10K` ORDER BY `id_i`").futureSummary() );
      }
      for( QuerySummaryFuture future : futures ) {
        QuerySummary summary = future.get( );
        System.out.print( summary.queryIdString() + ": " );
        if (summary.failed()) {
          System.out.println( "Error - " + summary.error().getMessage() );
        } else {
          System.out.println( summary.recordCount() );
        }
      }
    }
  }

}
