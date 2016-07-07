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

import java.util.List;
import java.util.Set;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.work.foreman.DrillbitStatusListener;
import org.apache.drill.yarn.zk.ZKClusterCoordinatorDriver;
import org.apache.drill.yarn.zk.ZKConfigException;
import org.apache.drill.yarn.zk.ZKRuntimeException;

public class ZKBasics
{
  public class StatusListener implements DrillbitStatusListener
  {
    @Override
    public void drillbitUnregistered(Set<DrillbitEndpoint> unregisteredDrillbits) {
      for (DrillbitEndpoint dbe : unregisteredDrillbits) {
        System.out.println("Deregistered: " + ZKClusterCoordinatorDriver.asString(dbe));
      }
    }

    @Override
    public void drillbitRegistered(Set<DrillbitEndpoint> registeredDrillbits) {
      for (DrillbitEndpoint dbe : registeredDrillbits) {
        System.out.println("Registered: " + ZKClusterCoordinatorDriver.asString(dbe));
      }
    }
  }

//  @SuppressWarnings("serial")
// public static class ZKException extends Exception
// {
//
//      public ZKException(String msg, Exception e) {
//        super( msg, e );
//      }
//
//  }

  private ZKClusterCoordinatorDriver driver;

  public static void main(String args[]) {
    try {
      (new ZKBasics()).run();
    } catch (ZKConfigException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ZKRuntimeException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private void run() throws ZKConfigException, ZKRuntimeException {
    // config = DrillConfig.create("/Users/progers/play/apache-drill-1.5.0/conf/drill-override.conf");
//     File drillDir = new File( "/Users/progers/play/apache-drill-1.6.0" );
//     File drillDir = new File( "/Users/progers/git/drill/distribution/target/apache-drill-1.7.0-SNAPSHOT/apache-drill-1.7.0-SNAPSHOT" );
    String connect = "localhost:2181/drill/paul";
    driver = new ZKClusterCoordinatorDriver().setConnect(connect)
        .setFailureTimoutMs( 30_000 ).build();
    driver.addDrillbitListener(new StatusListener());
    // config = DrillConfig.create("/Users/progers/play/apache-drill-1.5.0/conf/drill-override.conf");
//     try {
////     this.clusterCoordinator = ClusterCoordinatorFactory.instance( drillDir, connect );
////     this.clusterCoordinator = new MyZKClusterCoordinator(this.config, connect);
////     this.clusterCoordinator = new ZKClusterCoordinator(connect);
////     this.clusterCoordinator.start(10000);
//      } catch (Exception e) {
//        throw new ZKException("Failure setting up ZK for client.", e);
//     }

    final List<DrillbitEndpoint> endpoints = driver.getInitialEndpoints();
    for (DrillbitEndpoint dbe : endpoints) {
      System.out.println("Initial: " + ZKClusterCoordinatorDriver.asString(dbe));
    }

    int runTimeMs = 120_000;
    int stepTime = 10_000;
    int count = runTimeMs / stepTime;
    for (;;) {
      try {
        Thread.sleep(stepTime);
        if (driver.hasFailed()) {
          System.err.println("ZK has failed, stopping.");
          break;
        }
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      if (--count == 0) {
        break; }
    }
    driver.close();
    }
}
