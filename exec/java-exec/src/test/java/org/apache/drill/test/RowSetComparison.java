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
package org.apache.drill.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.drill.exec.vector.accessor.ColumnReader;
import org.apache.drill.test.TestRowSet.RowSetReader;
import org.bouncycastle.util.Arrays;

/**
 * For testing, compare the contents of two row sets (record batches)
 * to verify that they are identical. Supports masks to exclude certain
 * columns from comparison.
 */

public class RowSetComparison {

  private TestRowSet expected;
  private boolean mask[];
  private double delta = 0.001;

  public RowSetComparison(TestRowSet expected) {
    this.expected = expected;
    mask = new boolean[expected.schema().count()];
    for (int i = 0; i < mask.length; i++) {
      mask[i] = true;
    }
  }

  public RowSetComparison exclude(int colNo) {
    mask[colNo] = false;
    return this;
  }

  public RowSetComparison withMask(Boolean...flags) {
    for (int i = 0; i < flags.length; i++) {
      mask[i] = flags[i];
    }
    return this;
  }

  public RowSetComparison withDelta(double delta) {
    this.delta = delta;
    return this;
  }

  public void verify(TestRowSet actual) {
    assertEquals("Row counts", expected.rowCount(), actual.rowCount());
    RowSetReader er = expected.reader();
    RowSetReader ar = actual.reader();
    while (er.next()) {
      ar.next();
      verifyRow(er, ar);
    }
  }

  public void verifyAndClear(TestRowSet actual) {
    try {
      verify(actual);
    } finally {
      expected.clear();
      actual.clear();
    }
  }

  private void verifyRow(RowSetReader er, RowSetReader ar) {
    for (int i = 0; i < mask.length; i++) {
      if (! mask[i]) {
        continue;
      }
      ColumnReader ec = er.column(i);
      ColumnReader ac = ar.column(i);
      String label = er.index() + ":" + i;
      if (ec.isNull()) {
        assertTrue(label + " - column not null", ac.isNull());
        continue;
      }
      if (! ec.isNull()) {
        assertTrue(label + " - column is null", ! ac.isNull());
      }
      switch (ec.getType()) {
      case BYTES: {
        byte expected[] = ac.getBytes();
        byte actual[] = ac.getBytes();
        assertEquals(label + " - byte lengths differ", expected.length, actual.length);
        assertTrue(label, Arrays.areEqual(expected, actual));
        break;
     }
     case DOUBLE:
       assertEquals(label, ec.getDouble(), ac.getDouble(), delta);
       break;
      case INTEGER:
        assertEquals(label, ec.getInt(), ac.getInt());
        break;
      case LONG:
        assertEquals(label, ec.getLong(), ac.getLong());
        break;
      case STRING:
        assertEquals(label, ec.getString(), ac.getString());
        break;
      case DECIMAL:
        assertEquals(label, ec.getDecimal(), ac.getDecimal());
        break;
      default:
        throw new IllegalStateException( "Unexpected type: " + ec.getType());
      }
    }
  }
}
