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

import java.util.List;

import org.apache.drill.exec.record.metadata.ColumnMetadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Structure of a mock table definition file. Yes, using Jackson deserialization to parse
 * the file is brittle, but this is for testing so we're favoring convenience
 * over robustness.
 */

@JsonTypeName("mock-table")
public class MockTableDef {

  public static final String NULL_RATE_PROP = "nullRate";
  public static final String REPEAT_PROP = "repeat";
  public static final String GENERATOR_PROP = "generator";
  public static final String SPAN_PROP = "span";
  public static final String DELTA_PROP = "delta";

  /**
   * Describes one simulated file (or block) within the logical file scan
   * described by this group scan. Each block can have a distinct schema to test
   * for schema changes.
   */

  public static class MockScanEntry {

    protected final int records;
    private final boolean extended;
    private final int batchSize;
    private final int repeat;
    private final ColumnMetadata[] types;

    @JsonCreator
    public MockScanEntry(@JsonProperty("records") int records,
                         @JsonProperty("extended") Boolean extended,
                         @JsonProperty("batchSize") Integer batchSize,
                         @JsonProperty("repeat") Integer repeat,
                         @JsonProperty("types") ColumnMetadata[] types) {
      this.records = records;
      this.types = types;
      this.extended = (extended == null) ? false : extended;
      this.batchSize = (batchSize == null) ? 0 : batchSize;
      this.repeat = (repeat == null) ? 1 : repeat;
    }

    public int getRecords() { return records; }
    public boolean isExtended() { return extended; }
    public int getBatchSize() { return batchSize; }
    public int getRepeat() { return repeat; }

    public ColumnMetadata[] getTypes() {
      return types;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder()
          .append(getClass().getSimpleName())
          .append(" [records=")
          .append(getRecords())
          .append(", columns=[");
      if (types != null) {
        for (int i = 0; i < types.length; i++) {
          if (i > 0) {
            buf.append(", ");
          }
          buf.append(types[i].planString());
        }
      }
      return buf.append("]]").toString();
    }
  }

  private final String descrip;
  protected List<MockTableDef.MockScanEntry> entries;

  public MockTableDef(@JsonProperty("descrip") final String descrip,
                      @JsonProperty("entries") final List<MockTableDef.MockScanEntry> entries) {
    this.descrip = descrip;
    this.entries = entries;
  }

  /**
   * Description of this data source. Ignored by the scanner, purely
   * for the convenience of the author.
   */

  public String getDescrip() { return descrip; }

  /**
   * The set of entries that define the groups within the file. Each
   * group can have a distinct schema; each may be read in a separate
   * fragment.
   * @return
   */

  public List<MockTableDef.MockScanEntry> getEntries() { return entries; }
}
