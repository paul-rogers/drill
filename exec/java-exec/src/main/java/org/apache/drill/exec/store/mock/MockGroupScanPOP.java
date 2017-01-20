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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.mock.MockTableDef.MockColumn;
import org.apache.drill.exec.store.mock.MockTableDef.MockScanEntry;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;

/**
 * Describes a "group" scan of a (logical) mock table. The mock table has a
 * schema described by the {@link MockScanEntry}. Class. To simulate a scan that
 * can be parallelized, this group scan can contain a list of
 * {@link MockScanEntry}, each of which simulates a separate file on disk, or
 * block within a file. Each will give rise to a separate minor fragment
 * (assuming sufficient parallelization.)
 */

@JsonTypeName("mock-scan")
public class MockGroupScanPOP extends AbstractGroupScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
      .getLogger(MockGroupScanPOP.class);

  /**
   * URL for the scan. Unused. Appears to be a vestige of an earlier design that
   * required them.
   */
  private final String url;

  /**
   * The set of simulated files to scan.
   */
  protected final List<MockTableDef.MockScanEntry> readEntries;
  private LinkedList<MockTableDef.MockScanEntry>[] mappings;

  /**
   * Whether this group scan uses a newer "extended" schema definition, or the
   * original (non-extended) definition.
   */

  private boolean extended;
  private ScanStats scanStats = ScanStats.TRIVIAL_TABLE;

  @JsonCreator
  public MockGroupScanPOP(@JsonProperty("url") String url,
                          @JsonProperty("entries") List<MockTableDef.MockScanEntry> readEntries) {
    super((String)null);
    this.readEntries = readEntries;
    this.url = url;

    int rowCount = 0;
    int rowWidth = 0;
    for (MockScanEntry entry : readEntries) {
      rowCount += entry.getRecords();
      int width = 0;
      for (MockColumn col : entry.getTypes()) {
        int colWidth = 0;
        if ( col.getWidth() == 0 ) {
          colWidth = TypeHelper.getSize(col.getMajorType());
        } else {
          colWidth = col.getWidth();
        }
        colWidth *= col.getRepeatCount();
        width += colWidth;
      }
      rowWidth = Math.max(rowWidth, width);
    }
    int dataSize = rowCount * rowWidth;
    scanStats = new ScanStats( GroupScanProperty.EXACT_ROW_COUNT,
                               rowCount,
                               DrillCostBase.BASE_CPU_COST * dataSize,
                               DrillCostBase.BYTE_DISK_READ_COST * dataSize);
  }

  @Override
  public ScanStats getScanStats() {
    return scanStats;
  }

  public String getUrl() {
    return url;
  }

  @JsonProperty("entries")
  public List<MockTableDef.MockScanEntry> getReadEntries() {
    return readEntries;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) {
    Preconditions.checkArgument(endpoints.size() <= getReadEntries().size());

    mappings = new LinkedList[endpoints.size()];

    int i = 0;
    for (MockTableDef.MockScanEntry e : this.getReadEntries()) {
      if (i == endpoints.size()) {
        i -= endpoints.size();
      }
      LinkedList<MockTableDef.MockScanEntry> entries = mappings[i];
      if (entries == null) {
        entries = new LinkedList<MockTableDef.MockScanEntry>();
        mappings[i] = entries;
      }
      entries.add(e);
      i++;
    }
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) {
    assert minorFragmentId < mappings.length : String.format(
        "Mappings length [%d] should be longer than minor fragment id [%d] but it isn't.",
        mappings.length, minorFragmentId);
    return new MockSubScanPOP(url, extended, mappings[minorFragmentId]);
  }

  @Override
  public int getMaxParallelizationWidth() {
    return readEntries.size();
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new MockGroupScanPOP(url, readEntries);
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    if (columns.isEmpty()) {
      throw new IllegalArgumentException("No columns for mock scan");
    }
    List<MockTableDef.MockColumn> mockCols = new ArrayList<>( );
    Pattern p = Pattern.compile("(\\w+)_([isd])(\\d*)");
    for (SchemaPath path : columns) {
      String col = path.getLastSegment().getNameSegment().getPath();
      if (col.equals("*")) {
        return this;
      }
      Matcher m = p.matcher(col);
      if (!m.matches()) {
        throw new IllegalArgumentException(
            "Badly formatted mock column name: " + col);
      }
      @SuppressWarnings("unused")
      String name = m.group(1);
      String type = m.group(2);
      String length = m.group(3);
      int width = 10;
      if (!length.isEmpty()) {
        width = Integer.parseInt(length);
      }
      MinorType minorType;
      switch (type) {
      case "i":
        minorType = MinorType.INT;
        break;
      case "s":
        minorType = MinorType.VARCHAR;
        break;
      case "d":
        minorType = MinorType.FLOAT8;
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported field type " + type + " for mock column " + col);
      }
      MockTableDef.MockColumn mockCol = new MockTableDef.MockColumn(
          col, minorType, DataMode.REQUIRED, width, 0, 0, null, 1, null);
      mockCols.add(mockCol);
    }
    MockTableDef.MockScanEntry entry = readEntries.get(0);
    MockTableDef.MockColumn types[] = new MockTableDef.MockColumn[mockCols.size()];
    mockCols.toArray(types);
    MockTableDef.MockScanEntry newEntry = new MockTableDef.MockScanEntry( entry.records, true, 0, types );
    List<MockTableDef.MockScanEntry> newEntries = new ArrayList<>( );
    newEntries.add(newEntry);
    return new MockGroupScanPOP( url, newEntries );
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public String toString() {
    return "MockGroupScanPOP [url=" + url + ", readEntries=" + readEntries
        + "]";
  }

  @Override
  @JsonIgnore
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }
}
