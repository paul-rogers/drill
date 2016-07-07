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
package org.apache.drill.yarn.mock;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;

public class ZKPrototype
{
  public static String connectString = "localhost:2181";

  public static final String REGISTRY_NODE = "/registry";
  public static final String SERVICE_NODE = "test";
  public static final String SERVICE_REGISTRY_PATH = REGISTRY_NODE + "/" + SERVICE_NODE;
  public static final String METADATA_NODE = "/metadata";
  public static final String SERVICE_METADATA_PATH = METADATA_NODE + "/" + SERVICE_NODE;
  public static final String CHILD_NODE = "child";
  public static final String CHILD_PATH = SERVICE_METADATA_PATH + "/" + CHILD_NODE;

  public static final String SERVICE_CREATE_CONTENT = "create";
  public static final String SERVICE_MODIFY_CONTENT = "update";
  public static final String CREATE_CHILD_CONTENT = "child_create";
  public static final String MODIFY_CHILD_CONTENT = "child_update";

  public static int counter = 0;
  public static final int START_STEP = counter;
  public static final int CREATE_REGISTER_STEP = ++counter;

  public static final int CREATE_SERVICE_STEP = ++counter;
  public static final int POST_CREATE_SERVICE_STEP = ++counter;

  public static final int MODIFY_SERVICE_STEP = ++counter;
  public static final int POST_MODIFY_SERVICE_STEP = ++counter;

  public static final int CREATE_SERVICE_MD_STEP = ++counter;
  public static final int POST_CREATE_SERVICE_MD_STEP = ++counter;

  public static final int CREATE_MD_CHILD_STEP = ++counter;
  public static final int POST_MD_CREATE_CHILD_STEP = ++counter;

  public static final int MODIFY_MD_CHILD_STEP = ++counter;
  public static final int INTERMED_MD_MODIFY_CHILD_STEP = ++counter;
  public static final int POST_MODIFY_MD_CHILD_STEP = ++counter;

  public static final int DELETE_MD_CHILD_STEP = ++counter;
  public static final int POST_DELETE_MD_CHILD_STEP = ++counter;

  public static final int WRITER_DISCONNECT_STEP = ++counter;
  public static final int POST_DELETE_STEP = ++counter;

  public static class Stepper
  {
    private volatile int step;

    public synchronized void advanceTo( int to ) {
      assert( to > step );
      step = to;
      System.out.println( "Thread : " + Thread.currentThread().getName() + " --> " + to );
      notifyAll( );
    }

    public synchronized void advanceBy( int by ) {
      step += by;
      notifyAll( );
    }

    public synchronized void waitFor( int target ) throws InterruptedException {
      while ( step < target ) {
        System.out.println( "Thread : " + Thread.currentThread().getName() + " wait for " + target );
        wait( );
      }
    }

    public synchronized int getState() {
      System.out.println( "Thread : " + Thread.currentThread().getName() + " get " + step );
      return step;
    }

    public void reset() {
      step = 0;
    }
  }

  public abstract static class BaseWorker extends Thread
  {
    protected CuratorFramework client;
    protected Stepper stepper;

    public BaseWorker( String name, Stepper stepper ) {
      super( name );
      this.stepper = stepper;
    }

    @Override
    public void run( ) {
      try {
        setup( );
        doTest( );
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    protected void setup( ) {
      client = connectToZk( );
    }

    protected abstract void doTest() throws Exception;
  }

  public static class Writer extends BaseWorker
  {
    public Writer(Stepper stepper) {
      super("Writer", stepper);
    }

    @Override
    protected void doTest( ) throws Exception
    {
      try {
        client.create( ).withMode(CreateMode.PERSISTENT).forPath( METADATA_NODE, new byte[0] );
      } catch ( NodeExistsException e ) {
        // OK
      }
      try {
        client.create( ).withMode(CreateMode.PERSISTENT).forPath( REGISTRY_NODE, new byte[0] );
      } catch ( NodeExistsException e ) {
        // OK
      }
      stepper.waitFor( CREATE_REGISTER_STEP );
      synchronized ( stepper ) {
        client.create( ).withMode(CreateMode.EPHEMERAL).forPath( SERVICE_REGISTRY_PATH, encode( SERVICE_CREATE_CONTENT ) );
        stepper.advanceTo( CREATE_SERVICE_STEP );
      }
      stepper.waitFor( POST_CREATE_SERVICE_STEP );

      synchronized ( stepper ) {
        client.setData().forPath(SERVICE_REGISTRY_PATH, encode( SERVICE_MODIFY_CONTENT ) );
        stepper.advanceTo( MODIFY_SERVICE_STEP );
      }
      stepper.waitFor( POST_MODIFY_SERVICE_STEP );

      synchronized ( stepper ) {
        client.create( ).withMode(CreateMode.PERSISTENT).forPath( SERVICE_METADATA_PATH, new byte[0] );
        stepper.advanceTo( CREATE_SERVICE_MD_STEP );
      }
      stepper.waitFor( POST_CREATE_SERVICE_MD_STEP );

      synchronized ( stepper ) {
        client.create( ).withMode(CreateMode.EPHEMERAL).forPath( CHILD_PATH, encode( CREATE_CHILD_CONTENT ) );
        stepper.advanceTo( CREATE_MD_CHILD_STEP );
      }
      stepper.waitFor( POST_MD_CREATE_CHILD_STEP );

      synchronized ( stepper ) {
        client.setData().forPath(CHILD_PATH, encode( MODIFY_CHILD_CONTENT ) );
        stepper.advanceTo( MODIFY_MD_CHILD_STEP );
      }
      stepper.waitFor( POST_MODIFY_MD_CHILD_STEP );

      synchronized ( stepper ) {
        client.delete().forPath( CHILD_PATH );
        stepper.advanceTo( DELETE_MD_CHILD_STEP );
      }
      stepper.waitFor( POST_DELETE_MD_CHILD_STEP );

      synchronized ( stepper ) {
        client.close();
        stepper.advanceTo( WRITER_DISCONNECT_STEP );
      }
    }
  }

  public static class Reader extends BaseWorker
  {
    public Reader( Stepper stepper ) {
      super( "Reader", stepper );
    }

    @Override
    protected void doTest( ) throws Exception
    {
      try {
        client.create( ).withMode(CreateMode.PERSISTENT).forPath( METADATA_NODE, new byte[0] );
      } catch ( NodeExistsException e ) {
        // OK
      }
      try {
        client.create( ).withMode(CreateMode.PERSISTENT).forPath( REGISTRY_NODE, new byte[0] );
      } catch ( NodeExistsException e ) {
        // OK
      }
      List<String> result = client.getChildren( ).usingWatcher( makeRegistryChildWatcher1() ).forPath( REGISTRY_NODE );
      assert( result.isEmpty() );
      result = client.getChildren( ).usingWatcher( makeMetadataChildWatcher() ).forPath( METADATA_NODE );
      assert( result.isEmpty() );
      stepper.advanceTo( CREATE_REGISTER_STEP );
      stepper.waitFor( POST_DELETE_STEP );
    }

    private CuratorWatcher makeRegistryChildWatcher1() {
      return new CuratorWatcher( ) {

        @Override
        public void process(WatchedEvent event) throws Exception {
          assert( stepper.getState( ) == CREATE_SERVICE_STEP );
          assert( event.getType() == EventType.NodeChildrenChanged );
          List<String> result = client.getChildren( ).forPath( REGISTRY_NODE );
          assert ( result.contains( SERVICE_NODE ) );
          byte[] data = client.getData( ).usingWatcher( makeServiceNodeModifyWatcher( ) ).forPath( SERVICE_REGISTRY_PATH );
          assert( SERVICE_CREATE_CONTENT.equals( decode( data ) ) );
          stepper.advanceTo( POST_CREATE_SERVICE_STEP );
        }
      };
    }

    private CuratorWatcher makeServiceNodeModifyWatcher() {
      return new CuratorWatcher( ) {

        @Override
        public void process(WatchedEvent event) throws Exception {
          assert( stepper.getState( ) == MODIFY_SERVICE_STEP );
          assert( event.getType() == EventType.NodeDataChanged );
          byte[] data = client.getData( ).usingWatcher( makeServiceDeleteWatcher() ).forPath( SERVICE_REGISTRY_PATH );
          assert( SERVICE_MODIFY_CONTENT.equals( decode( data ) ) );
          List<String> result = client.getChildren( ).usingWatcher( makeMetadataChildWatcher() ).forPath( METADATA_NODE );
          assert( result.isEmpty() );
          stepper.advanceTo( POST_MODIFY_SERVICE_STEP );
        }
      };
    }

    private CuratorWatcher makeMetadataChildWatcher() {
      return new CuratorWatcher( ) {

        @Override
        public void process(WatchedEvent event) throws Exception {
//          assert( stepper.getState( ) == CREATE_SERVICE_MD_STEP );

          // Called twice for some bizarre reason.

          if ( stepper.getState( ) != CREATE_SERVICE_MD_STEP ) {
            return;
          }
          assert( event.getType() == EventType.NodeChildrenChanged );
          List<String> result = client.getChildren( ).usingWatcher( makeChildNodeDeleteWatcher() ).forPath( METADATA_NODE );
          assert( result.contains( SERVICE_NODE ) );
          result = client.getChildren( ).usingWatcher( makeServiceMetadataChildWatcher() ).forPath( SERVICE_METADATA_PATH );
          assert( result.isEmpty() );
          stepper.advanceTo( POST_CREATE_SERVICE_MD_STEP );
        }
      };
    }

    private CuratorWatcher makeServiceMetadataChildWatcher() {
      return new CuratorWatcher( ) {

        @Override
        public void process(WatchedEvent event) throws Exception {
//          assert( stepper.getState( ) == CREATE_MD_CHILD_STEP );

          // Called twice for some reason.

          if ( stepper.getState( ) != CREATE_MD_CHILD_STEP ) {
            return;
          }
          assert( event.getType() == EventType.NodeChildrenChanged );
          List<String> result = client.getChildren( ).usingWatcher( makeChildNodeDeleteWatcher() ).forPath( SERVICE_METADATA_PATH );
          assert( result.contains( CHILD_NODE ) );
          byte[] data = client.getData( ).usingWatcher( makeChildModifyWatcher() ).forPath( CHILD_PATH );
          assert( CREATE_CHILD_CONTENT.equals( decode( data ) ) );
          stepper.advanceTo( POST_MD_CREATE_CHILD_STEP );
        }
      };
    }

    private CuratorWatcher makeChildModifyWatcher() {
      return new CuratorWatcher( ) {

        @Override
        public void process(WatchedEvent event) throws Exception {
//          assert( stepper.getState( ) == MODIFY_MD_CHILD_STEP );

          // Called twice.

          if ( stepper.getState( ) != MODIFY_MD_CHILD_STEP ) {
            return;
          }
          assert( event.getType() == EventType.NodeDataChanged );
          byte[] data = client.getData( ).forPath( CHILD_PATH );
          assert( MODIFY_CHILD_CONTENT.equals( decode( data ) ) );
          stepper.advanceTo( POST_MODIFY_MD_CHILD_STEP );
        }
      };
    }

    private CuratorWatcher makeChildNodeDeleteWatcher() {
      return new CuratorWatcher( ) {

        @Override
        public void process(WatchedEvent event) throws Exception {
//          assert( stepper.getState( ) == DELETE_MD_CHILD_STEP );
          if ( stepper.getState( ) != DELETE_MD_CHILD_STEP ) {
            return;
          }
          assert( event.getType() == EventType.NodeChildrenChanged );
          List<String> result = client.getChildren( ).forPath( SERVICE_METADATA_PATH );
          assert( result.isEmpty( ) );
          stepper.advanceTo( POST_DELETE_MD_CHILD_STEP );
        }
      };
    }

    private CuratorWatcher makeServiceDeleteWatcher() {
      return new CuratorWatcher( ) {

        @Override
        public void process(WatchedEvent event) throws Exception {
          assert( event.getType() == EventType.NodeDeleted );
          stepper.advanceTo( POST_DELETE_STEP );
        }
      };
    }
  }

  CuratorFramework cf;
  Stepper stepper = new Stepper( );

  public static void main(String[] args) {
    new ZKPrototype( ).run( );
  }

  public static CuratorFramework connectToZk() {
    CuratorFramework client = CuratorFrameworkFactory.builder()
        .namespace("PaulTest")
        .connectString(connectString)
        .retryPolicy(new RetryNTimes( 3, 1000 ))
        .build( );
    client.start( );
    return client;
  }

  public static byte[] encode( String contents ) {
    try {
      return contents.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
      System.exit( 1 );
      return null;
    }
  }

  public static String decode( byte[] data ) {
    try {
      return new String( data, "UTF-8" );
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
      System.exit( 1 );
      return null;
    }
  }

  private void run() {
    setup( );
    try {
      doWork( );
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    tearDown( );
  }

  private void setup() {
    CuratorFramework client = connectToZk( );
    Stepper stepper = new Stepper( );
    try {
      client.delete().deletingChildrenIfNeeded().inBackground(new DeleteCallback( ), stepper).forPath( REGISTRY_NODE );
      stepper.waitFor( 1 );
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    stepper.reset( );
    try {
      client.delete().deletingChildrenIfNeeded().inBackground(new DeleteCallback( ), stepper).forPath( METADATA_NODE );
      stepper.waitFor( 1 );
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    client.close();
  }

  private static class DeleteCallback implements BackgroundCallback
  {

    @Override
    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
      Stepper stepper = (Stepper) event.getContext();
      stepper.advanceTo( 1 );
    }

  }

  private void doWork() throws InterruptedException {
    Writer writer = new Writer( stepper );
    Reader reader = new Reader( stepper );
    writer.start();
    reader.start();
    writer.join();
    reader.join();
  }

  private void tearDown() {
    // TODO Auto-generated method stub

  }

}
