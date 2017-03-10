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

import org.apache.drill.exec.record.selection.SelectionVector2;

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
}
