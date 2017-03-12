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
package org.apache.drill.exec.vector.accessor;

import java.math.BigDecimal;
import java.time.LocalDate;

public class AccessorUtilities {

  private AccessorUtilities() { }

  public static void setFromInt(ColumnWriter writer, int value) {
    switch (writer.getType()) {
    case BYTES:
      writer.setBytes(Integer.toHexString(value).getBytes());
      break;
    case DOUBLE:
      writer.setDouble(value);
      break;
    case INTEGER:
      writer.setInt(value);
      break;
    case LONG:
      writer.setLong(value);
      break;
    case STRING:
      writer.setString(Integer.toString(value));
      break;
    case DECIMAL:
      writer.setDecimal(BigDecimal.valueOf(value));
      break;
    default:
      throw new IllegalStateException("Unknown writer type: " + writer.getType());
    }
  }

  public static int sv4Batch(int sv4Index) {
    return sv4Index >>> 16;
  }

  public static int sv4Index(int sv4Index) {
    return sv4Index & 0xFFFF;
  }

  // Borrowed from the Parquet reader.
  /**
   * Number of days between Julian day epoch (January 1, 4713 BC) and Unix day epoch (January 1, 1970).
   * The value of this constant is {@value}.
   */
  public static final long JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH = 2440588;

  /**
   * Conversion from Java {@link LocalDate} epoch days to Drill DATE.
   *
   * @param value days since the Java epoch (1970-01-01)
   * @return days since the Drill epoch (4713BC-01-01)
   */

  public static long javaToDrillEpochDays(long value) {
    return value - JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH;
  }

  /**
   * Conversion from Drill DATE to Java {@link LocalDate}
   * epoch days.
   *
   * @param value days since the Drill epoch (4713BC-01-01)
   * @return days since the Java epoch (1970-01-01)
   */

  public static long drillToJavaEpochDays(long value) {
    return value + JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH;
  }

  /**
   * Conversion from Drill DATE to Java {@link LocalDate}.
   *
   * @param value days since the Drill epoch (4713BC-01-01)
   * @return the local date
   */

  public static LocalDate drillDateToLocalDate(long value) {
    return LocalDate.ofEpochDay(drillToJavaEpochDays(value));
  }

  /**
   * Conversion from Java {@link LocalDate} to Drill DATE.
   *
   * @param value the local date
   * @return days since the Drill epoch (4713BC-01-01)
   */

  public static long drillDateToLocalDate(LocalDate value) {
    return javaToDrillEpochDays(value.toEpochDay());
  }

  /**
   * Number of milliseconds between the Java epoch of
   * 1970-01-01T00:00:00 and the Drill epoch 0f
   * 2001-01-01T00:00:00. This is the same as the Drill
   * epoch represented in a Java millisecond timestamp.
   */

  public static final long DRILL_EPOCH_OFFSET_MS = 978_307_200_000L;

  /**
   * Convert a Java timestamp in ms into a Drill timestamp for the
   * Drill DATETIME type.
   * @param value the time expressed as milliseconds since
   * 1970-01-01T00:00:00 in an unspecified timezone.
   * @return the time expressed as milliseconds since
   * 2001-01-01T00:00:00 in an unspecified timezone.
   */

  public static long javaToDrillEpochMs(long value) {
    return value - DRILL_EPOCH_OFFSET_MS;
  }

  /**
   * Convert a Drill timestamp for the Drill DATETIME type to
   * a Java timestamp in ms.
   * @param value the time expressed as milliseconds since
   * 2001-01-01T00:00:00 in an unspecified timezone.
   * @return the time expressed as milliseconds since
   * 1970-01-01T00:00:00 in an unspecified timezone.
   */

  public static long drillToJavaEpochMs(long value) {
    return value + DRILL_EPOCH_OFFSET_MS;
  }

//
//  public static long drillToJavaEpochDays(long value) {
//    return value + DRILL_EPOCH_OFFSET_DAYS;
//  }
//
//  public static long javaToDrillEpochDays(long value) {
//    return value - DRILL_EPOCH_OFFSET_DAYS;
//  }
//
//  public static long drillToJavaEpochDays
//  public static DateTime dateToDateTime(xx value) {
//
//  }
//
//  public static xxx dateTimeToDate(DateTime value) {
//
//  }
//
//  public static LocalTime timeToLocalTime(xx value) {
//
//  }
//
//  public static xxx timeToLocalTime(LocalTime value) {
//
//  }


}
