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
package org.apache.drill.exec.physical.resultSet.project;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents one level of qualifier for a column. Analogous to
 * a {@code SchemaPath}, but represents the result of coalescing
 * multiple occurrences of the same column.
 */
public abstract class Qualifier {

  public abstract RequestedColumn.PathType pathType();

  /**
   * A array qualifier with one or more array entries.
   * For example: {@code a[1, 3, 5]}.
   */
  public static class SelectedArrayQualifier extends Qualifier {
    private final Set<Integer> indexes = new HashSet<>();

    @Override
    public RequestedColumn.PathType pathType() { return RequestedColumn.PathType.ARRAY; }

    public Set<Integer> indexes() { return indexes; }

    public boolean hasIndex(int index) {
      return indexes.contains(index);
    }

    public int maxIndex() {
      int max = 0;
      for (final Integer index : indexes) {
        max = Math.max(max, index);
      }
      return max;
    }

    public boolean[] indexArray() {
      final int max = maxIndex();
      final boolean map[] = new boolean[max+1];
      for (final Integer index : indexes) {
        map[index] = true;
      }
      return map;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder()
          .append("[");
      List<String> idxs = indexes.stream().sorted().map(i -> Integer.toString(i)).collect(Collectors.toList());
      return buf.append(String.join(", ", idxs))
         .append("]")
         .toString();
    }
  }

  /**
   * Special marker to indicate that that a) the item is an array, and b) that
   * all indexes are to be projected. Used when seeing both {@code a} and
   * {@code a[x]}. That is, the entire array is projected: {@code a[*]}.
   */
  public static class FullArrayQualifier extends Qualifier {

    @Override
    public RequestedColumn.PathType pathType() { return RequestedColumn.PathType.ARRAY; }

    @Override
    public String toString() {
      return "[*]";
    }
  }

  /**
   * The column is projected with nested members, and thus must
   * be a {@code MAP} or {@code ARRAY<MAP>}. This form tracks the
   * specific members projected, that is {@code a.{b,c,d}}.
   */
  public static class MapQualifier extends Qualifier {
    private final RequestedTuple members;

    public MapQualifier(RequestedTuple members) {
      this.members = members;
    }

    @Override
    public RequestedColumn.PathType pathType() { return RequestedColumn.PathType.MAP; }

    public RequestedTuple members() { return members; }

    @Override
    public String toString() {
      return "{" + members.toString() + "}";
    }
  }
}
