/*******************************************************************************
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
package org.apache.drill;

import org.apache.drill.ClusterFixture.FixtureBuilder;
import org.apache.drill.exec.ExecTest;

/**
 * Base class for tests that use the Drill engine for testing.
 * Use this class if you want to control startng and stopping
 * the embedded Drillbit(s). Use {@link BaseTestQuery} if you
 * want the test to do this work for you. Use this class if you
 * want to set up custom configurations, use BaseTestQuery if
 * you want to use the standard configuration.
 * <p>
 * Using BaseTestQuery when you want a custom configuration
 * requires your test to pay the cost of starting and stopping
 * Drillbits that your test does not actually use.
 */

public class DrillEngineTest extends ExecTest {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillEngineTest.class);

  public static FixtureBuilder newBuilder( ) {
    return ClusterFixture.builder( );
  }

  public static ClusterFixture standardCluster( ) throws Exception {
    return newBuilder( ).build( );
  }

}
