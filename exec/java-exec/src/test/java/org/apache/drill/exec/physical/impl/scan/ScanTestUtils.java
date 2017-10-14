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
package org.apache.drill.exec.physical.impl.scan;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsArrayParser;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsArrayProjection;
import org.apache.drill.exec.physical.impl.scan.file.FileLevelProjection;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataColumnDefn;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataColumnsParser;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataProjection;
import org.apache.drill.exec.physical.impl.scan.file.ResolvedPartitionColumn;
import org.apache.drill.exec.physical.impl.scan.project.ColumnProjection;
import org.apache.drill.exec.physical.impl.scan.project.ResolvedColumn;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanProjectionBuilder;
import org.apache.drill.exec.record.ColumnMetadata;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleSchema;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.hadoop.fs.Path;

public class ScanTestUtils {

  // Default file metadata column names; primarily for testing.

  public static final String FILE_NAME_COL = "filename";
  public static final String FULLY_QUALIFIED_NAME_COL = "fqn";
  public static final String FILE_PATH_COL = "filepath";
  public static final String SUFFIX_COL = "suffix";
  public static final String PARTITION_COL = "dir";

  public static class ProjectionFixture {

    public final ScanProjectionBuilder scanBuilder;
    public FileMetadataColumnsParser metdataParser;
    public ScanLevelProjection scanProj;
    public FileMetadataProjection metadataProj;
    private ColumnsArrayParser colArrayParser;
    private ColumnsArrayProjection colArrayProj;

    public ProjectionFixture() {
      scanBuilder = new ScanProjectionBuilder();
    }

    public ProjectionFixture withFileParser(OptionSet options) {
      metdataParser = new FileMetadataColumnsParser(options);
      scanBuilder.addParser(metdataParser);
      return this;
    }

    public ProjectionFixture withColumnsArrayParser() {
      colArrayParser = new ColumnsArrayParser();
      scanBuilder.addParser(colArrayParser);
      return this;
    }

    public ProjectionFixture projectedCols(String... cols) {
      scanBuilder.projectedCols(projectList(cols));
      return this;
    }

    public ScanLevelProjection build() {

      // Build the planner and verify

      scanProj = scanBuilder.build();
      if (metdataParser != null) {
        metadataProj = metdataParser.getProjection();
      }
      if (colArrayParser != null) {
        colArrayProj = colArrayParser.getProjection();

        // Temporary

        if (metadataProj != null) {
          metadataProj.bind(colArrayProj);
        }
      }
      return scanProj;
    }

    public FileLevelProjection resolve(Path path) {
      return metadataProj.resolve(scanProj, path);
    }

    public FileLevelProjection resolve(String path) {
      return resolve(new Path(path));
    }

    /**
     * Mimic legacy wildcard expansion of metadata columns. Is not a full
     * emulation because this version only works if the wildcard was at the end
     * of the list (or alone.)
     * @param scanProj scan projection definition (provides the partition column names)
     * @param base the table part of the expansion
     * @param dirCount number of partition directories
     * @return schema with the metadata columns appended to the table columns
     */

    public TupleMetadata expandMetadata(TupleMetadata base, int dirCount) {
      TupleMetadata metadataSchema = new TupleSchema();
      for (ColumnMetadata col : base) {
        metadataSchema.addColumn(col);
      }
      for (FileMetadataColumnDefn fileColDefn : metadataProj.fileMetadataColDefns()) {
        metadataSchema.add(MaterializedField.create(fileColDefn.colName(), fileColDefn.dataType()));
      }
      for (int i = 0; i < dirCount; i++) {
        metadataSchema.add(MaterializedField.create(metadataProj.partitionName(i),
            ResolvedPartitionColumn.dataType()));
      }
      return metadataSchema;
    }
  }

  static List<SchemaPath> projectList(String... names) {
    List<SchemaPath> selected = new ArrayList<>();
    for (String name: names) {
      selected.add(SchemaPath.getSimplePath(name));
    }
    return selected;
  }

  public static String partitionColName(int partition) {
    return PARTITION_COL + partition;
  }

  public static TupleMetadata schema(List<? extends ColumnProjection> output) {
    TupleMetadata schema = new TupleSchema();
    for (ColumnProjection col : output) {
      if (col.resolved()) {
        schema.add(((ResolvedColumn) col).schema());
      } else {
        schema.add(MaterializedField.create(col.name(),
            MajorType.newBuilder()
            .setMinorType(MinorType.NULL)
            .setMode(DataMode.OPTIONAL)
            .build()));
      }
    }
    return schema;
  }
}
