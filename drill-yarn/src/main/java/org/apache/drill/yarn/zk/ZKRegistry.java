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
package org.apache.drill.yarn.zk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.work.foreman.DrillbitStatusListener;
import org.apache.drill.yarn.appMaster.AMWrapperException;
import org.apache.drill.yarn.appMaster.ClusterController;
import org.apache.drill.yarn.appMaster.DispatcherAddOn;
import org.apache.drill.yarn.appMaster.EventContext;
import org.apache.drill.yarn.appMaster.Pollable;
import org.apache.drill.yarn.appMaster.Task;
import org.apache.drill.yarn.appMaster.TaskLifecycleListener;
import org.apache.hadoop.yarn.api.records.Container;

/**
 * AM-specific implementation of a Drillbit registry backed
 * by ZooKeeper. Listens to ZK events for registering a Drillbit
 * and deregistering. Alerts the Cluster Controller of these events.
 * Note, however, that the Cluster Controller <b>does not</b> send
 * events to this registry: doing so would introduce a possible
 * deadlock. This registry is aware ONLY of ZK events, not of the
 * YARN state of a node.
 */

public class ZKRegistry implements TaskLifecycleListener, DrillbitStatusListener,
                                   Pollable, DispatcherAddOn
{
  /**
   * State of each Drillbit that we've discovered through ZK or
   * launched via the AM. The tracker is where we combine the
   * ZK information with AM to correlate overall Drillbit
   * health.
   */

  private static class DrillbitTracker
  {
    /**
     * A Drillbit can be in one of four states.
     */

    public enum State {

      /**
       * An unmanaged Drillbit is one that has announced itself
       * via ZK, but which the AM didn't launch (or has not yet received
       * confirmation from YARN that it was launched.) In the normal
       * state, this state either does not occur (YARN reports the task
       * launch before the Drillbit registers in ZK) or is transient
       * (if the Drillbit registers in ZK before YARN gets around to telling
       * the AM that the Drillbit was launched.) A Drillbit that stays in
       * the unregistered state is likely one launched outside the AM:
       * either launched manually or (possibly), one left from a previous,
       * failed AM run (though YARN is supposed to kill left-over child
       * processes in that case.)
       */

      UNMANAGED,

      /**
       * A new Drillbit is one that the AM has launched, but that has
       * not yet registered itself with ZK. This is normally a transient
       * state that occurs as ZK registration catches up with the YARN
       * launch notification. If a Drillbit says in this state, then something
       * is seriously wrong (perhaps a mis-configuration). The cluster controller
       * will patiently wait a while, then decide bad things are happening and
       * will ask YARN to kill the Drillbit, then will retry a time or two,
       * after which it will throw up its hands, blacklist the node, and wait
       * for the admin to sort things out.
       */

      NEW,

      /**
       * Normal operating state: the AM launched the Drillbit, which then
       * dutifully registered itself in ZK. Nothing to see here, move along.
       */

      REGISTERED,

      /**
       * The Drillbit was working just fine, but its registration has dropped out
       * of ZK for a reason best left to the cluster controller to determine.
       * Perhaps the controller has decided to kill the Drillbit. Perhaps the
       * Drillbit became unresponsive (in which case the controller will kill it
       * and retry) or has died (in which case YARN will alert the AM that the
       * process exited.)
       */

      DEREGISTERED
    }

    /**
     * The common key used between tasks and ZK registrations. The key is of
     * the form:<br>
     * <pre>host:port:port:port</pre>
     */

    protected final String key;

    /**
     * ZK tracking state.
     * @see {@link State}
     */

    protected State state;

    /**
     * For Drillbits started by the AM, the task object for this Drillbit.
     */

    protected Task task;

    /**
     * For Drillbits discovered through ZK, the Drill endpoint for the
     * Drillbit.
     */

    protected DrillbitEndpoint endpoint;

    public DrillbitTracker(String key) {
      this.key = key;
      this.state = DrillbitTracker.State.UNMANAGED;
    }

    public DrillbitTracker(String key, Task task) {
      this.key = key;
      this.task = task;
      state = DrillbitTracker.State.NEW;
    }

    /**
     * Mark that a YARN-managed task has become registered in ZK. This
     * indicates that the task has come online. Tell the task to update
     * its state to record that the task
     * is, in fact, registered in ZK. This indicates a normal, healthy
     * task.
     *
     * @param tracker
     */

    private void becomeRegistered( ClusterController controller ) {
      state = DrillbitTracker.State.REGISTERED;
      task.properties.put( ENDPOINT_PROPERTY, endpoint );
      EventContext context = new EventContext( controller );
      context.setTask( task );
      context.getState().startAck( context );
    }

    /**
     * Mark that a YARN-managed Drillbit has dropped out of ZK.
     * @param controller
     */

    public void becomeUnregistered(ClusterController controller)
    {
      assert state == DrillbitTracker.State.REGISTERED;
      state = DrillbitTracker.State.DEREGISTERED;
      task.properties.remove( ENDPOINT_PROPERTY );
      EventContext context = new EventContext( controller );
      context.setTask( task );
      context.getState().completionAck( context );
    }

  }

  public static final String CONTROLLER_PROPERTY = "zk";

  // TODO: longer for production

  public static final int UPDATE_PERIOD_MS = 20_000;

  public static final String ENDPOINT_PROPERTY = "endpoint";

  private static final Log LOG = LogFactory.getLog(ZKRegistry.class);
  public Map<String,DrillbitTracker> registry = new HashMap<>( );
  private ZKClusterCoordinatorDriver zkDriver;
  private ClusterController controller;
  private long lastUpdateTime;

  public ZKRegistry( ZKClusterCoordinatorDriver zkDriver ) {
    this.zkDriver = zkDriver;
  }

  @Override
  public void start(ClusterController controller) {
    this.controller = controller;
    try {
      zkDriver.build();
    } catch (ZKRuntimeException e) {
      LOG.error( "Failed to start ZK monitoring", e);
      throw new AMWrapperException( "Failed to start ZK monitoring", e );
    }
    for ( DrillbitEndpoint dbe : zkDriver.getInitialEndpoints() ) {
      String key = toKey( dbe );
      registry.put( key, new DrillbitTracker( key ) );

      // Blacklist the host for each unmanaged drillbit.

      controller.reserveHost( dbe.getAddress() );

      LOG.warn( "Host " + dbe.getAddress() + " already running a Drillbit outside of YARN." );
    }
    zkDriver.addDrillbitListener( this );
  }

  /**
   * Convert a Drillbit endpoint to a string key used in the
   * (zk-->task) map. Note that the string format here must match
   * the one used in {@link #toKey(Task)} to map a task to string key.
   *
   * @param dbe the drillbit endpoint from ZK
   * @return a string key for this object
   */

  private String toKey( DrillbitEndpoint dbe ) {
    return ZKClusterCoordinatorDriver.asString( dbe );
  }

  /**
   * Convert a task to a string key used in the
   * (zk-->task) map. Note that the string format here must match
   * the one used in {@link #toKey(DrillbitEndpoint)} to map a
   * drillbit endpoint to string key.
   *
   * @param task the task tracked by the cluster controller
   * @return a string key for this object
   */

  private String toKey(Task task) {
    return toKey( task.getContainer() );
  }

  private String toKey( Container container )
  {
    return zkDriver.toKey( container.getNodeId().getHost() );
  }

  /**
   * Callback from ZK to indicate that one or more drillbits have become registered.
   */

  @Override
  public synchronized void drillbitRegistered(Set<DrillbitEndpoint> registeredDrillbits) {
    for ( DrillbitEndpoint dbe : registeredDrillbits ) {
      drillbitRegistered( dbe );
    }
  }

  /**
   * Called when a drillbit has become registered.
   * There are two cases. Either this is a normal registration of a previously-started
   * task, or this is an unmanaged drillbit for which we have no matching task.
   */

  private void drillbitRegistered(DrillbitEndpoint dbe) {
    String key = toKey( dbe );
    DrillbitTracker tracker = registry.get( key );
    if ( tracker == null ) {
      // Unmanaged drillbit case

      LOG.info( "Registration of unmanaged drillbit: " + key );
      tracker = new DrillbitTracker( key );
      tracker.endpoint = dbe;
      registry.put( key, tracker );
      return;
    }

    // Managed drillbit case. Might be we lost, then regained
    // ZK connection.

    if ( tracker.state == DrillbitTracker.State.REGISTERED ) {
      return; }

    // Otherwise, the Drillbit has just registered with ZK.
    // Or, if the ZK connection was lost and regained, the
    // state changes from DEREGISTERED --> REGISTERED

    tracker.endpoint = dbe;
    tracker.becomeRegistered( controller );
  }

  /**
   * Callback from ZK to indicate that one or more drillbits have become
   * deregistered from ZK.
   */

  @Override
  public synchronized void drillbitUnregistered(Set<DrillbitEndpoint> unregisteredDrillbits) {
    for ( DrillbitEndpoint dbe : unregisteredDrillbits ) {
      drillbitUnregistered( dbe );
    }
  }

  /**
   * Handle the case that a drillbit becomes unregistered.
   * There are three cases.
   * <ol>
   * <li>The deregistration is for a drillbit that is not in
   * the registry table. Indicates a code error.</li>
   * <li>The drillbit is unmanaged. This occurs for drillbits
   * started and stopped outside of YARN.</li>
   * <li>Normal case of deregistration of a YARN-managed
   * drillbit. Inform the controller of this event.</li>
   * </ol>
   *
   * @param dbe
   */

  private void drillbitUnregistered(DrillbitEndpoint dbe) {
    String key = toKey( dbe );
    DrillbitTracker tracker = registry.get( key );
    assert tracker != null;
    if ( tracker == null ) {
      // Something is terribly wrong.

      LOG.error( "Internal error - Unexpected drillbit unregistration: " + key );
      return;
    }
    if ( tracker.state == DrillbitTracker.State.UNMANAGED ) {
      // Unmanaged drillbit

      assert tracker.task == null;
      LOG.info( "Unmanaged drillbit unregistered: " + key );
      registry.remove( key );
      controller.releaseHost( dbe.getAddress() );
      return;
    }
    tracker.becomeUnregistered( controller );
  }

  /**
   * Listen for selected YARN task state changes.
   *
   */
  @Override
  public synchronized void stateChange(Event event, EventContext context) {
    switch ( event ) {
    case ALLOCATED:
      taskCreated( context.task );
      break;
//    case STARTED:
//      taskStarted( task );
//      break;
    case ENDED:
      if ( context.task.getTrackingState() == Task.TrackingState.END_ACK ) {
        taskEnded( context.task );
      }
      break;
    default:
      break;
    }
  }

  /**
   * Indicates that the cluster controller has created a task that
   * we expect to be monitored by ZK. We handle two cases: the normal
   * case in which we later receive a ZK notification. And, the
   * unusual case in which we've already received the ZK notification
   * and we now match that notification with this task. (The second
   * case could occur if latency causes us to receive the ZK
   * notification before we learn from the NM that the task is
   * alive.)
   *
   * @param task
   */

  private void taskCreated(Task task) {
    String key = toKey( task );
    DrillbitTracker tracker = registry.get(key);
    if ( tracker == null ) {
      // Normal case: no ZK registration yet.

      registry.put( key, new DrillbitTracker( key, task ) );
    }
    else if ( tracker.state == DrillbitTracker.State.UNMANAGED ) {
      // Unusual case: ZK registration came first.

      LOG.info( "Unmanaged drillbit became managed: " + key );
      tracker.task = task;
      tracker.becomeRegistered( controller );
    }
    else {
      LOG.error( task.getLabel() + " - Drillbit registry in wrong state " + tracker.state + " for new task: " + key );
    }
  }

  /**
   * Mark that a task (YARN container) has ended. Updates the
   * (zk --> task) registry by removing the task. The
   * cluster controller state machine monitors ZK and does not
   * end the task until the ZK registration for that task drops.
   * As a result, the entry here should be in the deregistered state
   * or something is seriously wrong.
   *
   * @param task
   */

  private void taskEnded(Task task) {
    String key = toKey( task );
    DrillbitTracker tracker = registry.get(key);
    assert tracker != null;
    assert tracker.state == DrillbitTracker.State.DEREGISTERED;
    registry.remove( key );
  }

  /**
   * Periodically check ZK status. If the ZK connection has timed out,
   * something is very seriously wrong. Shut the whole Drill cluster
   * down since Drill cannot operate without ZooKeeper.
   */

  @Override
  public void tick(long curTime) {
    if ( lastUpdateTime + UPDATE_PERIOD_MS < curTime ) {
      return; }

    lastUpdateTime = curTime;
    if ( zkDriver.hasFailed() ) {
      int secs = (int) ((zkDriver.getLostConnectionDurationMs() + 500) / 1000 );
      LOG.error( "ZooKeeper connection lost, failing after " + secs + " seconds." );
      controller.shutDown();
    }
  }

  @Override
  public void finish(ClusterController controller) {
    zkDriver.removeDrillbitListener( this );
    zkDriver.close();
  }

  public synchronized List<String> listUnmanagedDrillits() {
    List<String> drillbits = new ArrayList<>( );
    for ( DrillbitTracker item : registry.values() ) {
      if ( item.state == DrillbitTracker.State.UNMANAGED ) {
        drillbits.add( item.key );
      }
    }
    // TESTING
//    drillbits.add( "foo:123:456:789" );
    return drillbits;
  }
}
