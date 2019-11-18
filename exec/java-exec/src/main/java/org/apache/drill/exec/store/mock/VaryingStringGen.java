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
package org.apache.drill.exec.store.mock;

import java.util.Random;

import org.apache.drill.exec.vector.accessor.ScalarWriter;


public class VaryingStringGen extends AbstractFieldGen {

  private final Random rand = new Random();
  private int length;
  private int span;
  private int deltaPerSpan;
  private int valueCount;

  @Override
  public void setup(ColumnDef colDef, ScalarWriter colLoader) {
    super.setup(colDef, colLoader);
    length = colDef.getConfig().precision();
    span = colDef.mockCol.intProperty(MockTableDef.SPAN_PROP, 1000);
    deltaPerSpan = colDef.mockCol.intProperty(MockTableDef.DELTA_PROP, 100);
  }

  public String value() {
    if (valueCount++ >= span) {
      valueCount = 0;
      length = Math.max(0, length + deltaPerSpan);
    }
    String c = Character.toString((char) (rand.nextInt(26) + 'A'));
    StringBuilder buf = new StringBuilder();
    for (int i = 0;  i < length;  i++) {
      buf.append(c);
    }
    return buf.toString();
  }

  @Override
  public void setValue() {
    colWriter.setString(value());
  }
}
