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
package org.apache.drill.yarn.core;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.hadoop.yarn.api.records.Container;

public class DoYUtil {

  private DoYUtil() {
  }

  public static String join(String separator, List<String> list) {
    StringBuilder buf = new StringBuilder();
    String sep = "";
    for (String item : list) {
      buf.append(sep);
      buf.append(item);
      sep = separator;
    }
    return buf.toString();
  }

  public static void addNonEmpty(List<String> list, String value) {
    if (value == null) {
      return;
    }
    value = value.trim();
    if (value.isEmpty()) {
      return;
    }
    list.add(value);
  }

  public static boolean isBlank( String str ) {
    return str == null ||  str.trim().isEmpty();
  }

  public static String toIsoTime( long timestamp ) {

    // Uses old-style dates rather than java.time because
    // the code still must compile for JDK 7.

    DateFormat fmt = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
    fmt.setTimeZone(TimeZone.getDefault());
    return fmt.format( new Date( timestamp ) );
  }
  /**
   * Utility method to display YARN container information in a
   * useful way for log messages.
   * @param container
   * @return
   */

  public static String describeContainer(Container container) {
    StringBuilder buf = new StringBuilder( )
       .append( "Host: " )
       .append( container.getNodeHttpAddress() )
       .append( ", Memory: " )
       .append( container.getResource().getMemory() )
       .append( " MB, Vcores: " )
       .append( container.getResource().getVirtualCores() );
    return buf.toString();
  }

  /**
   * The tracking URL given to YARN is a redirect URL. When giving the URL
   * to the user, "unwrap" that redirect URL to get the actual site URL.
   *
   * @param trackingUrl
   * @return
   */

  public static String unwrapAmUrl( String trackingUrl ) {
    return  trackingUrl.replace( "/redirect", "/" );
  }
}
