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
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsArrayParser;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsArrayProjection;
import org.apache.drill.exec.physical.impl.scan.file.FileLevelProjection;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataColumnDefn;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataManager;
import org.apache.drill.exec.physical.impl.scan.file.ResolvedPartitionColumn;
import org.apache.drill.exec.physical.impl.scan.project.NoOpReaderProjection;
import org.apache.drill.exec.physical.impl.scan.project.ReaderLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ScanProjectionParser;
import org.apache.drill.exec.record.ColumnMetadata;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleSchema;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

public class ScanTestUtils {

  // Default file metadata column names; primarily for testing.

  public static final String FILE_NAME_COL = "filename";
  public static final String FULLY_QUALIFIED_NAME_COL = "fqn";
  public static final String FILE_PATH_COL = "filepath";
  public static final String SUFFIX_COL = "suffix";
  public static final String PARTITION_COL = "dir";

  public static class ProjectionFixture {

    public List<SchemaPath> projection = new ArrayList<>();
    public ScanLevelProjection scanProj;
    public boolean useFileManager;
    public OptionSet options;
    public boolean useLegacyWildcardExpansion;
    public Path rootDir;
    public FileMetadataManager metadataProj;
    private ColumnsArrayParser colArrayParser;
    private ColumnsArrayProjection colArrayProj;

    public ProjectionFixture() {
    }

    public ProjectionFixture withFileManager(OptionSet options) {
       this.options = options;
      useFileManager = true;
      return this;
    }

    public ProjectionFixture useLegacyWildcardExpansion(boolean flag) {
      useLegacyWildcardExpansion = flag;
      return this;

    }

    public ProjectionFixture setScanRootDir(Path path) {
      rootDir = path;
      return this;
    }

    public ProjectionFixture withColumnsArrayParser() {
      colArrayParser = new ColumnsArrayParser();
      return this;
    }

    public ProjectionFixture projectedCols(String... cols) {
      projection = projectList(cols);
      return this;
    }

    public ProjectionFixture projectAll() {
      projection = ScanTestUtils.projectAll();
      return this;
    }

    public ScanLevelProjection build() {

      if (useFileManager) {
        metadataProj = new FileMetadataManager(options, useLegacyWildcardExpansion, rootDir);
      }

      // Build the planner and verify

      List<ScanProjectionParser> parsers = new ArrayList<>();
      if (metadataProj != null) {
        parsers.add(metadataProj.projectionParser());
      }
      if (colArrayParser != null) {
        parsers.add(colArrayParser);
      }

      scanProj = new ScanLevelProjection(projection, parsers);
      if (colArrayParser != null) {
        colArrayProj = colArrayParser.getProjection();

//        // Temporary
//
//        if (metadataProj != null) {
//          metadataProj.bind(colArrayProj);
//        }
      }
      return scanProj;
    }

    public ReaderLevelProjection resolveFile(Path path) {
      metadataProj.startFile(path);
      return metadataProj.resolve(scanProj);
    }

    public ReaderLevelProjection resolve(String path) {
      return resolveFile(new Path(path));
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

    public ReaderLevelProjection resolveReader() {
      return new NoOpReaderProjection(scanProj);
    }
  }

  static List<SchemaPath> projectList(String... names) {
    List<SchemaPath> selected = new ArrayList<>();
    for (String name: names) {
      selected.add(SchemaPath.getSimplePath(name));
    }
    return selected;
  }

  public static List<SchemaPath> projectAll() {
    return Lists.newArrayList(
        new SchemaPath[] {SchemaPath.getSimplePath(SchemaPath.WILDCARD)});
  }

  public static String partitionColName(int partition) {
    return PARTITION_COL + partition;
  }

//  public static TupleMetadata schema(List<? extends ColumnProjection> output) {
//    TupleMetadata schema = new TupleSchema();
//    for (ColumnProjection col : output) {
//      if (col.resolved()) {
//        schema.add(((ResolvedColumn) col).schema());
//      } else {
//        schema.add(MaterializedField.create(col.name(),
//            MajorType.newBuilder()
//            .setMinorType(MinorType.NULL)
//            .setMode(DataMode.OPTIONAL)
//            .build()));
//      }
//    }
//    return schema;
//  }
}
