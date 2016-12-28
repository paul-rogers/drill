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
package org.apache.drill.exec.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.commons.math3.util.Pair;
import org.apache.drill.common.concurrent.ExtendedLatch;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.util.RepeatTestRule.Repeat;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.physical.impl.ScreenCreator;
import org.apache.drill.exec.physical.impl.SingleSenderCreator.SingleSenderRootExec;
import org.apache.drill.exec.physical.impl.mergereceiver.MergingRecordBatch;
import org.apache.drill.exec.physical.impl.partitionsender.PartitionSenderRootExec;
import org.apache.drill.exec.physical.impl.partitionsender.PartitionerDecorator;
import org.apache.drill.exec.physical.impl.unorderedreceiver.UnorderedReceiverBatch;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.sql.DrillSqlWorker;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.ExceptionWrapper;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.rpc.ConnectionThrottle;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.store.pojo.PojoRecordReader;
import org.apache.drill.exec.testing.Controls;
import org.apache.drill.exec.testing.ControlsInjectionUtil;
import org.apache.drill.exec.util.Pointer;
import org.apache.drill.exec.work.foreman.Foreman;
import org.apache.drill.exec.work.foreman.ForemanException;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.drill.exec.work.fragment.FragmentExecutor;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.DrillTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Preconditions;

public class TestDrillbitResilience extends DrillTest {

  /*
   * Canned drillbit names.
   */
  private final static String DRILLBIT_ALPHA = "alpha";
  private final static String DRILLBIT_BETA = "beta";
  private final static String DRILLBIT_GAMMA = "gamma";

  /**
   * The number of times test (that are repeated) should be repeated.
   */
  private static final int NUM_RUNS = 3;

  /**
   * Note: Counting sys.memory executes a fragment on every drillbit. This is a better check in comparison to
   * counting sys.drillbits.
   */
  private static final String TEST_QUERY = "select * from sys.memory";

  private static ClusterFixture cluster;
  private static ClientFixture client;

  // Zookeeper is a global, shared resource used by all test clusters.

  @BeforeClass
  public static void startCluster() throws Exception {
    cluster = ClusterFixture.builder()
          .withZk()
          .withBits(new String[] {DRILLBIT_ALPHA, DRILLBIT_BETA, DRILLBIT_GAMMA})
          .configProperty(ExecConstants.PARQUET_PAGEREADER_ASYNC, false)
          .build( );
    client = cluster.clientFixture();
  }

  @AfterClass
  public static void stopCluster() throws Exception {
    client.close( );
    cluster.close( );
  }

  // Helper classes

  /**
   * Tests can use this listener to wait, until the submitted query completes or fails, by
   * calling #waitForCompletion.
   */
  private static class WaitUntilCompleteListener implements UserResultsListener {
    private final ExtendedLatch latch = new ExtendedLatch(1); // to signal completion
    protected QueryId queryId = null;
    protected volatile Pointer<Exception> ex = new Pointer<>();
    protected volatile QueryState state = null;
    protected final ClientFixture client;

    public WaitUntilCompleteListener(ClientFixture client) {
      this.client = client;
     }

    /**
     * Method that sets the exception if the condition is not met.
     */
    protected final void check(final boolean condition, final String format, final Object... args) {
      if (!condition) {
        ex.value = new IllegalStateException(String.format(format, args));
      }
    }

    /**
     * Method that cancels and resumes the query, in order.
     */
    protected final void cancelAndResume() {
      Preconditions.checkNotNull(queryId);
      final ExtendedLatch trigger = new ExtendedLatch(1);
      (new CancellingThread(client.client(), queryId, ex, trigger)).start();
      (new ResumingThread(client.client(), queryId, ex, trigger)).start();
    }

    @Override
    public void queryIdArrived(final QueryId queryId) {
      this.queryId = queryId;
    }

    @Override
    public void submissionFailed(final UserException ex) {
      this.ex.value = ex;
      state = QueryState.FAILED;
      latch.countDown();
    }

    @Override
    public void queryCompleted(final QueryState state) {
      this.state = state;
      latch.countDown();
    }

    @Override
    public void dataArrived(final QueryDataBatch result, final ConnectionThrottle throttle) {
      result.release();
    }

    public final Pair<QueryState, Exception> waitForCompletion() {
      latch.awaitUninterruptibly();
      return new Pair<>(state, ex.value);
    }
  }

  private static class ListenerThatCancelsQueryAfterFirstBatchOfData extends WaitUntilCompleteListener {

    private boolean cancelRequested = false;

    public ListenerThatCancelsQueryAfterFirstBatchOfData(ClientFixture client) {
      super(client);
    }

    @Override
    public void dataArrived(final QueryDataBatch result, final ConnectionThrottle throttle) {
      if (!cancelRequested) {
        check(queryId != null, "Query id should not be null, since we have waited long enough.");
        (new CancellingThread(client.client(), queryId, ex, null)).start();
        cancelRequested = true;
      }
      result.release();
    }
  }

  /**
   * Thread that cancels the given query id. After the cancel is acknowledged, the latch is counted down.
   */
  private static class CancellingThread extends Thread {
    private final QueryId queryId;
    private final Pointer<Exception> ex;
    private final ExtendedLatch latch;
    private final DrillClient drillClient;

    public CancellingThread(final DrillClient client, final QueryId queryId, final Pointer<Exception> ex, final ExtendedLatch latch) {
      this.queryId = queryId;
      this.ex = ex;
      this.latch = latch;
      this.drillClient = client;
    }

    @Override
    public void run() {
      final DrillRpcFuture<Ack> cancelAck = drillClient.cancelQuery(queryId);
      try {
        cancelAck.checkedGet();
      } catch (final RpcException ex) {
        this.ex.value = ex;
      }
      if (latch != null) {
        latch.countDown();
      }
    }
  }

  /**
   * Thread that resumes the given query id. After the latch is counted down, the resume signal is sent, until then
   * the thread waits without interruption.
   */
  private static class ResumingThread extends Thread {
    private final QueryId queryId;
    private final Pointer<Exception> ex;
    private final ExtendedLatch latch;
    private final DrillClient drillClient;

    public ResumingThread(final DrillClient client, final QueryId queryId, final Pointer<Exception> ex, final ExtendedLatch latch) {
      this.queryId = queryId;
      this.ex = ex;
      this.latch = latch;
      this.drillClient = client;
    }

    @Override
    public void run() {
      latch.awaitUninterruptibly();
      final DrillRpcFuture<Ack> resumeAck = drillClient.resumeQuery(queryId);
      try {
        resumeAck.checkedGet();
      } catch (final RpcException ex) {
        this.ex.value = ex;
      }
    }
  }

  // The tests themselves

  @Test
  public void settingNoOpInjectionsAndQuery() throws Exception {
    final long before = countAllocatedMemory(cluster);

    final String controls = Controls.newBuilder()
      .addExceptionOnBit(getClass(), "noop", RuntimeException.class, getEndpoint(cluster, DRILLBIT_BETA))
      .build();
    setControls(client, controls);
    final WaitUntilCompleteListener listener = new WaitUntilCompleteListener(client);
    client.queryBuilder().sql(TEST_QUERY).withListener(listener);
    final Pair<QueryState, Exception> pair = listener.waitForCompletion();
    assertStateCompleted(pair, QueryState.COMPLETED);

    final long after = countAllocatedMemory(cluster);
    assertEquals(String.format("Leaked %d bytes", after - before), before, after);
  }

  /**
   * Test throwing exceptions from sites within the Foreman class, as specified by the site
   * description
   *
   * @param desc site description
   */
  private static void testForeman(ClientFixture client, final String desc) {
    final String controls = Controls.newBuilder()
      .addException(Foreman.class, desc, ForemanException.class)
      .build();
    assertFailsWithException(client, controls, ForemanException.class, desc);
  }

  @Test
  @Repeat(count = NUM_RUNS)
  public void foreman_runTryBeginning() throws Exception {
    final long before = countAllocatedMemory(cluster);

    testForeman(client, "run-try-beginning");

    final long after = countAllocatedMemory(cluster);
    assertEquals(String.format("Leaked %d bytes", after - before), before, after);
  }

  @Test
  @Ignore // TODO(DRILL-3163, DRILL-3167)
  @Repeat(count = NUM_RUNS)
  public void foreman_runTryEnd() throws Exception {
      final long before = countAllocatedMemory(cluster);

      testForeman(client, "run-try-end");

      final long after = countAllocatedMemory(cluster);
      assertEquals(String.format("We are leaking %d bytes", after - before), before, after);
  }

  // To test pause and resume. Test hangs and times out if resume did not happen.
  @Test
  public void passThrough() throws Exception {
    final long before = countAllocatedMemory(cluster);

    final WaitUntilCompleteListener listener = new WaitUntilCompleteListener(client) {
      @Override
      public void queryIdArrived(final QueryId queryId) {
        super.queryIdArrived(queryId);
        final ExtendedLatch trigger = new ExtendedLatch(1);
        (new ResumingThread(client.client(), queryId, ex, trigger)).start();
        trigger.countDown();
      }
    };

    final String controls = Controls.newBuilder()
      .addPause(PojoRecordReader.class, "read-next")
      .build();
    setControls(client, controls);

    client.queryBuilder().sql(TEST_QUERY).withListener(listener);
    final Pair<QueryState, Exception> result = listener.waitForCompletion();
    assertStateCompleted(result, QueryState.COMPLETED);

    final long after = countAllocatedMemory(cluster);
    assertEquals(String.format("Leaked %d bytes", after - before), before, after);
  }

  // DRILL-3052: Since root fragment is waiting on data and leaf fragments are cancelled before they send any
  // data to root, root will never run. This test will timeout if the root did not send the final state to Foreman.
  // DRILL-2383: Cancellation TC 1: cancel before any result set is returned.
  @Test
  @Ignore // TODO(DRILL-3192)
  //@Repeat(count = NUM_RUNS)
  public void cancelWhenQueryIdArrives() throws Exception {
    final long before = countAllocatedMemory(cluster);

    final WaitUntilCompleteListener listener = new WaitUntilCompleteListener(client) {

      @Override
      public void queryIdArrived(final QueryId queryId) {
        super.queryIdArrived(queryId);
        cancelAndResume();
      }
    };

    final String controls = Controls.newBuilder()
      .addPause(FragmentExecutor.class, "fragment-running")
      .build();
    assertCancelledWithoutException(client, controls, listener);

    final long after = countAllocatedMemory(cluster);
    assertEquals(String.format("Leaked %d bytes", after - before), before, after);
  }

  // DRILL-2383: Cancellation TC 2: cancel in the middle of fetching result set
  @Test
  @Repeat(count = NUM_RUNS)
  public void cancelInMiddleOfFetchingResults() throws Exception {
    final long before = countAllocatedMemory(client.cluster());

    final WaitUntilCompleteListener listener = new WaitUntilCompleteListener(client) {
      private boolean cancelRequested = false;

      @Override
      public void dataArrived(final QueryDataBatch result, final ConnectionThrottle throttle) {
        if (!cancelRequested) {
          check(queryId != null, "Query id should not be null, since we have waited long enough.");
          cancelAndResume();
          cancelRequested = true;
        }
        result.release();
      }
    };

    // skip once i.e. wait for one batch, so that #dataArrived above triggers #cancelAndResume
    final String controls = Controls.newBuilder()
      .addPause(ScreenCreator.class, "sending-data", 1)
      .build();
    assertCancelledWithoutException(client, controls, listener);

    final long after = countAllocatedMemory(client.cluster());
    assertEquals(String.format("Leaked %d bytes", after - before), before, after);
  }

  // DRILL-2383: Cancellation TC 3: cancel after all result set are produced but not all are fetched
  @Test
  @Repeat(count = NUM_RUNS)
  public void cancelAfterAllResultsProduced() throws Exception {
    final long before = countAllocatedMemory(client.cluster());

    final WaitUntilCompleteListener listener = new WaitUntilCompleteListener(client) {
      private int count = 0;

      @Override
      public void dataArrived(final QueryDataBatch result, final ConnectionThrottle throttle) {
        if (++count == client.cluster().drillbits().size()) {
          check(queryId != null, "Query id should not be null, since we have waited long enough.");
          cancelAndResume();
        }
        result.release();
      }
    };

    final String controls = Controls.newBuilder()
      .addPause(ScreenCreator.class, "send-complete")
      .build();
    assertCancelledWithoutException(client, controls, listener);

    final long after = countAllocatedMemory(client.cluster());
    assertEquals(String.format("Leaked %d bytes", after - before), before, after);
  }

  //DRILL-2383: Cancellation TC 4: cancel after everything is completed and fetched

  @Test
  @Ignore("DRILL-3967")
  @Repeat(count = NUM_RUNS)
  public void cancelAfterEverythingIsCompleted() throws Exception {
    final long before = countAllocatedMemory(client.cluster());

    final WaitUntilCompleteListener listener = new WaitUntilCompleteListener(client) {
      private int count = 0;

      @Override
      public void dataArrived(final QueryDataBatch result, final ConnectionThrottle throttle) {
        if (++count == client.cluster().drillbits().size()) {
          check(queryId != null, "Query id should not be null, since we have waited long enough.");
          cancelAndResume();
        }
        result.release();
      }
    };

    final String controls = Controls.newBuilder()
      .addPause(Foreman.class, "foreman-cleanup")
      .build();
    assertCancelledWithoutException(client, controls, listener);

    final long after = countAllocatedMemory(client.cluster());
    assertEquals(String.format("Leaked %d bytes", after - before), before, after);
  }

  //DRILL-2383: Completion TC 1: success
  @Test
  public void successfullyCompletes() throws Exception {
    final long before = countAllocatedMemory(cluster);

    final WaitUntilCompleteListener listener = new WaitUntilCompleteListener(client);
    client.queryBuilder().sql(TEST_QUERY).withListener(listener);
    final Pair<QueryState, Exception> result = listener.waitForCompletion();
    assertStateCompleted(result, QueryState.COMPLETED);

    final long after = countAllocatedMemory(cluster);
    assertEquals(String.format("Leaked %d bytes", after - before), before, after);
  }

  //DRILL-2383: Completion TC 2: failed query - before query is executed - while sql parsing
  @Test
  public void failsWhenParsing() throws Exception {
    final long before = countAllocatedMemory(cluster);

    final String exceptionDesc = "sql-parsing";
    final Class<? extends Throwable> exceptionClass = ForemanSetupException.class;
    final String controls = Controls.newBuilder()
    .addException(DrillSqlWorker.class, exceptionDesc, exceptionClass)
      .build();
    assertFailsWithException(client, controls, exceptionClass, exceptionDesc);

    final long after = countAllocatedMemory(cluster);
    assertEquals(String.format("Leaked %d bytes", after - before), before, after);
  }

  // DRILL-2383: Completion TC 3: failed query - before query is executed - while sending fragments to other
  // drillbits
  @Test
  public void failsWhenSendingFragments() throws Exception {
    final long before = countAllocatedMemory(cluster);

    final String exceptionDesc = "send-fragments";
    final Class<? extends Throwable> exceptionClass = ForemanException.class;
    final String controls = Controls.newBuilder()
            .addException(Foreman.class, exceptionDesc, exceptionClass)
            .build();
    assertFailsWithException(client, controls, exceptionClass, exceptionDesc);

    final long after = countAllocatedMemory(cluster);
    assertEquals(String.format("Leaked %d bytes", after - before), before, after);
  }

  //DRILL-2383: Completion TC 4: failed query - during query execution
  @Test
  public void failsDuringExecution() throws Exception {
    final long before = countAllocatedMemory(cluster);

    final String exceptionDesc = "fragment-execution";
    final Class<? extends Throwable> exceptionClass = IOException.class;
    final String controls = Controls.newBuilder()
      .addException(FragmentExecutor.class, exceptionDesc, exceptionClass)
      .build();
    assertFailsWithException(client, controls, exceptionClass, exceptionDesc);

    final long after = countAllocatedMemory(cluster);
    assertEquals(String.format("We are leaking %d bytes", after - before), before, after);
  }

  /**
   * Test cancelling query interrupts currently blocked FragmentExecutor threads waiting for some event to happen.
   * Specifically tests cancelling fragment which has {@link MergingRecordBatch} blocked waiting for data.
   * @throws Exception
   */
  @Test
  @Repeat(count = NUM_RUNS)
  public void interruptingBlockedMergingRecordBatch() throws Exception {
    final long before = countAllocatedMemory(cluster);

    final String control = Controls.newBuilder()
      .addPause(MergingRecordBatch.class, "waiting-for-data", 1)
      .build();
    interruptingBlockedFragmentsWaitingForData(client, control);

    final long after = countAllocatedMemory(cluster);
    assertEquals(String.format("Leaked %d bytes", after - before), before, after);
  }

  /**
   * Test cancelling query interrupts currently blocked FragmentExecutor threads waiting for some event to happen.
   * Specifically tests cancelling fragment which has {@link UnorderedReceiverBatch} blocked waiting for data.
   * @throws Exception
   */
  @Test
  @Repeat(count = NUM_RUNS)
  public void interruptingBlockedUnorderedReceiverBatch() throws Exception {
    final long before = countAllocatedMemory(cluster);

    final String control = Controls.newBuilder()
      .addPause(UnorderedReceiverBatch.class, "waiting-for-data", 1)
      .build();
    interruptingBlockedFragmentsWaitingForData(client, control);

    final long after = countAllocatedMemory(cluster);
    assertEquals(String.format("Leaked %d bytes", after - before), before, after);
  }

  /**
   * Tests interrupting the fragment thread that is running {@link PartitionSenderRootExec}.
   * {@link PartitionSenderRootExec} spawns threads for partitioner. Interrupting fragment thread should also interrupt
   * the partitioner threads.
   * @throws Exception
   */
  @Test
  @Repeat(count = NUM_RUNS)
  public void interruptingPartitionerThreadFragment() throws Exception {
    client.alterSession(ExecConstants.SLICE_TARGET, 1);
    client.alterSession(PlannerSettings.HASHAGG.getOptionName(), true);
    client.alterSession(PlannerSettings.PARTITION_SENDER_SET_THREADS.getOptionName(), 6);
    try {
      final long before = countAllocatedMemory(client.cluster());

      final String controls = Controls.newBuilder()
          .addLatch(PartitionerDecorator.class, "partitioner-sender-latch")
          .addPause(PartitionerDecorator.class, "wait-for-fragment-interrupt", 1)
          .build();

      final String query = "SELECT sales_city, COUNT(*) cnt FROM cp.`region.json` GROUP BY sales_city";
      assertCancelledWithoutException(client, controls, new ListenerThatCancelsQueryAfterFirstBatchOfData(client), query);

      final long after = countAllocatedMemory(client.cluster());
      assertEquals(String.format("Leaked %d bytes", after - before), before, after);

    } finally {
      client.alterSession(ExecConstants.SLICE_TARGET, ExecConstants.SLICE_TARGET_DEFAULT);
      client.alterSession(PlannerSettings.HASHAGG.getOptionName(), PlannerSettings.HASHAGG.getDefault().bool_val);
      client.alterSession(PlannerSettings.PARTITION_SENDER_SET_THREADS.getOptionName(), PlannerSettings.PARTITION_SENDER_SET_THREADS.getDefault().num_val);
    }
  }

  private static void interruptingBlockedFragmentsWaitingForData(final ClientFixture client, final String control) throws RpcException {
    client.alterSession(ExecConstants.SLICE_TARGET, 1);
    client.alterSession(PlannerSettings.HASHAGG.getOptionName(), false);
    try {
      final String query = "SELECT sales_city, COUNT(*) cnt FROM cp.`region.json` GROUP BY sales_city";
      assertCancelledWithoutException(client, control, new ListenerThatCancelsQueryAfterFirstBatchOfData(client), query);
    }
    finally {
      client.alterSession(ExecConstants.SLICE_TARGET, ExecConstants.SLICE_TARGET_DEFAULT);
      client.alterSession(PlannerSettings.HASHAGG.getOptionName(), PlannerSettings.HASHAGG.getDefault().bool_val);
    }
  }

  @Test
  @Ignore // TODO(DRILL-3193)
  public void interruptingWhileFragmentIsBlockedInAcquiringSendingTicket() throws Exception {
    final long before = countAllocatedMemory(cluster);

    final String control = Controls.newBuilder()
      .addPause(SingleSenderRootExec.class, "data-tunnel-send-batch-wait-for-interrupt", 1)
      .build();
    assertCancelledWithoutException(client, control, new ListenerThatCancelsQueryAfterFirstBatchOfData(client));

    final long after = countAllocatedMemory(cluster);
    assertEquals(String.format("Leaked %d bytes", after - before), before, after);
  }

  @Test
  @Repeat(count = NUM_RUNS)
  public void memoryLeaksWhenCancelled() throws Exception {
    final long before = countAllocatedMemory(client.cluster());

    final String controls = Controls.newBuilder()
      .addPause(ScreenCreator.class, "sending-data", 1)
      .build();

    String query = ClusterFixture.loadResource("queries/tpch/09.sql");
    query = query.substring(0, query.length() - 1); // drop the ";"

    final WaitUntilCompleteListener listener = new WaitUntilCompleteListener(client) {
      private volatile boolean cancelRequested = false;

      @Override
      public void dataArrived(final QueryDataBatch result, final ConnectionThrottle throttle) {
        if (!cancelRequested) {
          check(queryId != null, "Query id should not be null, since we have waited long enough.");
          cancelAndResume();
          cancelRequested = true;
        }
        result.release();
      }
    };

    assertCancelledWithoutException(client, controls, listener, query);

    final long after = countAllocatedMemory(client.cluster());
    assertEquals(String.format("Leaked %d bytes", after - before), before, after);
  }

  @Test
  @Ignore // TODO(DRILL-3194)
  @Repeat(count = NUM_RUNS)
  public void memoryLeaksWhenFailed() throws Exception {
    final long before = countAllocatedMemory(client.cluster());

    final String exceptionDesc = "fragment-execution";
    final Class<? extends Throwable> exceptionClass = IOException.class;
    final String controls = Controls.newBuilder()
      .addException(FragmentExecutor.class, exceptionDesc, exceptionClass)
      .build();

    // Work around the Parquet Snappy problem described elsewhere.

//    String query = "select name_s50 from mock.`nation_100K` order by name_s50";

    String query = ClusterFixture.loadResource("queries/tpch/09.sql");
    query = query.substring(0, query.length() - 1); // drop the ";"

    assertFailsWithException(client, controls, exceptionClass, exceptionDesc, query);

    final long after = countAllocatedMemory(client.cluster());
    assertEquals(String.format("Leaked %d bytes", after - before), before, after);
  }

  @Test // DRILL-3065
  public void failsAfterMSorterSorting() throws Exception {
    doFailsAfterMSorterSorting(client, true);
    doFailsAfterMSorterSorting(client, false);
  }

  private void doFailsAfterMSorterSorting(ClientFixture client, boolean managedVersion) throws RpcException {
    client.alterSession(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED_OPTION.getOptionName(), ! managedVersion);
    try {
      // TODO: DRILL-5157: Using Parquet causes a failure in the Parquet async reader
      // on the Mac due to a missing Snappy decoder. Since we only need fake data, use
      // the mock data source instead.
      final String query = "select n_name from cp.`tpch/nation.parquet` order by n_name";
  //    final String query = "select name_s50 from mock.`nation_100K` order by name_s50";
      final Class<? extends Exception> typeOfException = RuntimeException.class;

      final long before = countAllocatedMemory(client.cluster());
      Class<?> opClass = managedVersion ?
            org.apache.drill.exec.physical.impl.xsort.managed.ExternalSortBatch.class :
            org.apache.drill.exec.physical.impl.xsort.ExternalSortBatch.class;
      String event = managedVersion ?
            org.apache.drill.exec.physical.impl.xsort.managed.ExternalSortBatch.INTERRUPTION_AFTER_SORT :
            org.apache.drill.exec.physical.impl.xsort.ExternalSortBatch.INTERRUPTION_AFTER_SORT;
      final String controls = Controls.newBuilder()
        .addException(opClass, event, typeOfException)
        .build();
      assertFailsWithException(client, controls, typeOfException, event, query);

      final long after = countAllocatedMemory(client.cluster());
      assertEquals(String.format("Leaked %d bytes", after - before), before, after);
    } finally {
      client.alterSession(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED_OPTION.getOptionName(), false);
    }
  }

  @Test // DRILL-3085
  public void failsAfterMSorterSetup() throws Exception {
    doFailsAfterMSorterSetup(client, true);
    doFailsAfterMSorterSetup(client, false);
  }

  private void doFailsAfterMSorterSetup(ClientFixture client, boolean managedVersion) throws RpcException {
    client.alterSession(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED_OPTION.getOptionName(), ! managedVersion);
    try {
      // TODO: DRILL-5157: Using Parquet causes a failure in the Parquet async reader
      // on the Mac due to a missing Snappy decoder. Since we only need fake data, use
      // the mock data source instead.
      final String query = "select n_name from cp.`tpch/nation.parquet` order by n_name";
  //    final String query = "select name_s50 from mock.`nation_100K` order by name_s50";
      final Class<? extends Exception> typeOfException = RuntimeException.class;

      final long before = countAllocatedMemory(client.cluster());
      Class<?> opClass = managedVersion ?
          org.apache.drill.exec.physical.impl.xsort.managed.ExternalSortBatch.class :
          org.apache.drill.exec.physical.impl.xsort.ExternalSortBatch.class;
      String event = managedVersion ?
          org.apache.drill.exec.physical.impl.xsort.managed.ExternalSortBatch.INTERRUPTION_AFTER_SETUP :
          org.apache.drill.exec.physical.impl.xsort.ExternalSortBatch.INTERRUPTION_AFTER_SETUP;
      final String controls = Controls.newBuilder()
          .addException(opClass, event, typeOfException)
          .build();
      assertFailsWithException(client, controls, typeOfException, event, query);

      final long after = countAllocatedMemory(client.cluster());
      assertEquals(String.format("Leaked %d bytes", after - before), before, after);
    } finally {
      client.alterSession(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED_OPTION.getOptionName(), false);
    }
  }

  // Helper methods

  /**
   * Get the endpoint for the drillbit, if it is running
   * @param name name of the drillbit
   * @return endpoint of the drillbit
   */
  @SuppressWarnings("resource")
  private static DrillbitEndpoint getEndpoint(ClusterFixture cluster, final String name) {
    final Drillbit drillbit = cluster.drillbit(name);
    if (drillbit == null) {
      throw new IllegalStateException("No Drillbit named \"" + name + "\" found.");
    }
    return drillbit.getContext().getEndpoint();
  }

  private static long countAllocatedMemory(ClusterFixture cluster) {
    // wait to make sure all fragments finished cleaning up
    try {
      Thread.sleep(2000);
    } catch (final InterruptedException e) {
      // just ignore
    }

    long allocated = 0;
    for (Drillbit bit : cluster.drillbits()) {
      allocated += bit.getContext().getAllocator().getAllocatedMemory();
    }

    return allocated;
  }

  private static void assertFailsWithException(final ClientFixture client, final String controls,
      final Class<? extends Throwable> exceptionClass,
      final String exceptionDesc) {
    assertFailsWithException(client, controls, exceptionClass, exceptionDesc, TEST_QUERY);
  }

  /**
   * Given a set of controls, this method ensures TEST_QUERY fails with the given class and desc.
   */
  private static void assertFailsWithException(final ClientFixture client, final String controls, final Class<? extends Throwable> exceptionClass,
                                               final String exceptionDesc, final String query) {
    setControls(client, controls);
    final WaitUntilCompleteListener listener = new WaitUntilCompleteListener(client);
    client.queryBuilder().sql(query).withListener(listener);
    final Pair<QueryState, Exception> result = listener.waitForCompletion();
    final QueryState state = result.getFirst();
    assertTrue(String.format("Query state should be FAILED (and not %s).", state), state == QueryState.FAILED);
    assertExceptionMessage(result.getSecond(), exceptionClass, exceptionDesc);
  }

  /**
   * Check that the injected exception is what we were expecting.
   *
   * @param throwable      the throwable that was caught (by the test)
   * @param exceptionClass the expected exception class
   * @param desc           the expected exception site description
   */
  private static void assertExceptionMessage(final Throwable throwable, final Class<? extends Throwable> exceptionClass,
                                             final String desc) {
    assertTrue("Throwable was not of UserException type.", throwable instanceof UserException);
    final ExceptionWrapper cause = ((UserException) throwable).getOrCreatePBError(false).getException();
    assertEquals("Exception class names should match.", exceptionClass.getName(), cause.getExceptionClass());
    assertEquals("Exception sites should match.", desc, cause.getMessage());
  }

  /**
   * Given the result of {@link WaitUntilCompleteListener#waitForCompletion}, this method fails if the completed state
   * is not as expected, or if an exception is thrown. The completed state could be COMPLETED or CANCELED. This state
   * is set when {@link WaitUntilCompleteListener#queryCompleted} is called.
   */
  private static void assertStateCompleted(final Pair<QueryState, Exception> result, final QueryState expectedState) {
    final QueryState actualState = result.getFirst();
    final Exception exception = result.getSecond();
    if (actualState != expectedState || exception != null) {
      fail(String.format("Query state is incorrect (expected: %s, actual: %s) AND/OR \nException thrown: %s",
        expectedState, actualState, exception == null ? "none." : exception));
    }
  }

  /**
   * Given a set of controls, this method ensures that the given query completes with a CANCELED state.
   */
  private static void assertCancelledWithoutException(final ClientFixture client, final String controls,
                                                      final WaitUntilCompleteListener listener,
                                                      final String query) {
    setControls(client, controls);

    client.queryBuilder().sql(query).withListener(listener);
    final Pair<QueryState, Exception> result = listener.waitForCompletion();
    assertStateCompleted(result, QueryState.CANCELED);
  }

  /**
   * Given a set of controls, this method ensures that the TEST_QUERY completes with a CANCELED state.
   */
  private static void assertCancelledWithoutException(final ClientFixture client, final String controls,
                                                      final WaitUntilCompleteListener listener) {
    assertCancelledWithoutException(client, controls, listener, TEST_QUERY);
  }

  /**
   * Set the given controls.
   */
  private static void setControls(final ClientFixture client, final String controls) {
    ControlsInjectionUtil.setControls(client.client(), controls);
  }

}
