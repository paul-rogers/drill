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
package org.apache.drill.test.rowSet;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.accessor.AccessorUtilities;
import org.apache.drill.exec.vector.accessor.ColumnAccessor.ValueType;
import org.apache.drill.exec.vector.accessor.ColumnWriter;
import org.apache.drill.test.rowSet.RowSet.RowSetWriter;
import org.joda.time.Duration;
import org.joda.time.Period;

public class RowSetUtilities {

  private RowSetUtilities() { }

  public static void reverse(SelectionVector2 sv2) {
    int count = sv2.getCount();
    for (int i = 0; i < count / 2; i++) {
      char temp = sv2.getIndex(i);
      int dest = count - 1 - i;
      sv2.setIndex(i, sv2.getIndex(dest));
      sv2.setIndex(dest, temp);
    }
  }

  /**
   * Set a test data value from an int. Uses the type information of the
   * column to handle interval types. Else, uses the value type of the
   * accessor. The value set here is purely for testing; the mapping
   * from ints to intervals has no real meaning.
   *
   * @param rowWriter
   * @param index
   * @param value
   */

  public static void setFromInt(RowSetWriter rowWriter, int index, int value) {
    ColumnWriter writer = rowWriter.row().column(index);
    if (writer.valueType() == ValueType.PERIOD) {
      setPeriodFromInt(writer, rowWriter.schema().column(index).getType().getMinorType(), value);
    } else {
      AccessorUtilities.setFromInt(writer, value);
    }
  }

  public static void setPeriodFromInt(ColumnWriter writer, MinorType minorType,
      int value) {
    switch (minorType) {
    case INTERVAL:
      writer.setPeriod(Duration.millis(value).toPeriod());
      break;
    case INTERVALYEAR:
      writer.setPeriod(Period.years(value / 12).withMonths(value % 12));
      break;
    case INTERVALDAY:
      int sec = value % 60;
      value = value / 60;
      int min = value % 60;
      value = value / 60;
      writer.setPeriod(Period.days(value).withMinutes(min).withSeconds(sec));
      break;
    default:
      throw new IllegalArgumentException("Writer is not an interval: " + minorType);
    }
  }
}
