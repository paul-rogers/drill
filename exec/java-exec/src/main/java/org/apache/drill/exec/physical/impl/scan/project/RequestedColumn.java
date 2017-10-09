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
package org.apache.drill.exec.physical.impl.scan.project;

import org.apache.drill.common.expression.SchemaPath;

/**
 * Definition of a column from the SELECT list. This is
 * the column before semantic analysis is done to determine
 * the meaning of the column name.
 */

public class RequestedColumn {
  private final SchemaPath column;
  protected ScanOutputColumn resolution;

  public RequestedColumn(SchemaPath col) {
    column = col;
  }

  /**
   * Returns the root name:
   * <ul>
   * <li>a: returns a</li>
   * <li>a.b: returns a</li>
   * <li>a[10]: returns a</li>
   * </ul>
   * @return root name
   */

  public String name() { return column.getRootSegment().getPath(); }

  /**
   * Return the output column to which this input column is projected.
   *
   * @return the corresponding output column
   */

  public ScanOutputColumn resolution() {
    return resolution;
  }

  /**
   * Return if this column is the special wildcard ("*") column which means to
   * project all table columns.
   *
   * @return true if the column is "*"
   */

  public boolean isWildcard() { return name().equals(ScanLevelProjection.WILDCARD); }

  public boolean isColumnsArray() {
    return name().equalsIgnoreCase(ColumnsArrayParser.COLUMNS_COL);
  }

  public boolean isArray() {
    return ! column.isSimplePath();
  }

  public int arrayIndex() {
    return column.getRootSegment().getChild().getArraySegment().getIndex();
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder()
      .append("[InputColumn path=\"")
      .append(column)
      .append("\", resolution=");
    if (resolution == null) {
      buf.append("null");
    } else {
      buf.append(resolution.toString());
    }
    buf.append("]");
    return buf.toString();
  }
}