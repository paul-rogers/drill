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

import java.net.InetAddress;
import java.net.UnknownHostException;

public class PropTester
{

  public static void main(String[] args) {
//    for ( Object key : System.getProperties().keySet() ) {
//      System.out.print( key );
//      System.out.print( " --> " );
//      System.out.println( System.getProperty( (String) key ) );
//    }
    try {
      System.out.println( "Local Host Addr: " + InetAddress.getLocalHost().getHostAddress() );
      System.out.println( "Local Host Name: " + InetAddress.getLocalHost().getCanonicalHostName() );
      InetAddress inetAddress = InetAddress.getByName("PROGERS-MPR13.local");
      System.out.println( "YARN-style Addr: " + inetAddress.getHostAddress() );
      System.out.println( "local host: " + InetAddress.getLocalHost());
    } catch (UnknownHostException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
