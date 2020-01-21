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

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.record.metadata.ColumnMetadata;

public class RequestedWildcardColumn extends BaseRequestedColumn {

  public RequestedWildcardColumn(RequestedTuple parent, String name) {
    super(parent, name);
  }

  @Override
  public PathType pathType(int posn) { return null; }

  @Override
  public boolean isWildcard() { return true; }

  @Override
  public boolean isSimple() { return true; }

  @Override
  public boolean isTuple() { return false; }

  @Override
  public RequestedTuple mapProjection() { return null; }

  @Override
  public boolean isArray() { return false; }

  @Override
  public boolean hasIndexes() { return false; }

  @Override
  public int maxIndex() { return 0; }

  @Override
  public boolean[] indexes() { return null; }

  @Override
  public boolean hasIndex(int index) { return false; }

  @Override
  public boolean isConsistentWith(ColumnMetadata col) { return true; }

  @Override
  public boolean isConsistentWith(MajorType type) { return true; }

  @Override
  public String summary() { return name(); }

  @Override
  public String toString() { return name(); }
}
