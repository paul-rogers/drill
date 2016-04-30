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

import java.util.HashMap;
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

public class ZKRegistry implements TaskLifecycleListener, DrillbitStatusListener,
                                   Pollable, DispatcherAddOn
{
  private static class DrillbitTracker
  {
    public enum State {
      UNMANAGED, NEW, REGISTERED, DEREGISTERED
    }

    @SuppressWarnings("unused")
    final String key;
    State state;
    Task task;
    DrillbitEndpoint endpoint;

    public DrillbitTracker(String key) {
      this.key = key;
      this.state = DrillbitTracker.State.UNMANAGED;
    }

    public DrillbitTracker(String key, Task task) {
      this.key = key;
      this.task = task;
      state = DrillbitTracker.State.NEW;
    }
  }

  // TODO: longer for production

  public static final int UPDATE_PERIOD_MS = 20_000;

  public static final String ENDPOINT_PROPERTY = "endpoint";

  private static final Log LOG = LogFactory.getLog(ZKRegistry.class);
  public Map<String,DrillbitTracker> registry = new HashMap<>( );
  private ZKClusterCoordinatorDriver zkDriver;
  private ClusterController controller;
  private long lastUpdateTime;

  // Put here for now. These should be either from the config or
  // assigned per task on task creation and passed to the bit.

  private int userPort = 31010;
  private int controlPort = 31011;
  private int dataPort = 31012;

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
    StringBuilder buf = new StringBuilder( );
    buf.append( container.getNodeId().getHost() )
       .append( ":" )
       .append( userPort )
       .append(':')
       .append( controlPort )
       .append(':')
       .append( dataPort );
       return buf.toString();
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
      registry.put( key, new DrillbitTracker( key ) );
      return;
    }

    // Managed drillbit case: should have no registration yet

    assert tracker.state == DrillbitTracker.State.NEW;
    assert tracker.task != null;
    assert tracker.endpoint == null;
    tracker.endpoint = dbe;
    becomeRegistered( tracker );
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

    // Normal case of a YARN-managed drill-bit.
    // Tell the task that we've received the completion ack
    // event.

    assert tracker.state == DrillbitTracker.State.REGISTERED;
    tracker.state = DrillbitTracker.State.DEREGISTERED;
    tracker.task.properties.remove( ENDPOINT_PROPERTY );
    EventContext context = new EventContext( controller );
    context.setTask( tracker.task );
    context.getState().completionAck( context );
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
      becomeRegistered( tracker );
    }
    else {
      LOG.error( "Drillbit registry in wrong state " + tracker.state + " for new task: " + key );
    }
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

  private void becomeRegistered(DrillbitTracker tracker) {
    tracker.state = DrillbitTracker.State.REGISTERED;
    tracker.task.properties.put( ENDPOINT_PROPERTY, tracker.endpoint );
    EventContext context = new EventContext( controller );
    context.setTask( tracker.task );
    context.getState().startAck( context );
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

//  @Override
//  public void decorateTaskModel(Task task, TaskModel model) {
////    for ( TaskModel model : models ) {
////      if ( model.container == null ) {
////        continue; }
////      String key = toKey( model.container );
////      DrillbitTracker tracker = registry.get(key);
////      if ( tracker != null ) {
////        model.endpoint = tracker.endpoint;
//      model.endpoint = (DrillbitEndpoint) task.properties.get( ZKRegistry.ENDPOINT_PROPERTY );
////    }
//  }
}
