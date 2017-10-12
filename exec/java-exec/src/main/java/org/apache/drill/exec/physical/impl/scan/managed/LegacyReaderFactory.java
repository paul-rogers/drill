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
package org.apache.drill.exec.physical.impl.scan.managed;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.scan.ReaderFactory;
import org.apache.drill.exec.physical.impl.scan.RowBatchReader;
import org.apache.drill.exec.physical.impl.scan.metadata.FileMetadataColumnsParser;
import org.apache.drill.exec.physical.impl.scan.metadata.FileMetadataColumnsParser.FileMetadataProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanProjectionBuilder;
import org.apache.drill.exec.physical.impl.scan.project.ScanProjector;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;

/**
 * Provides the row set mutator used to construct record batches.
 * <p>
 * Provides the option to continue a schema from one batch to the next.
 * This can reduce spurious schema changes in formats, such as JSON, with
 * varying fields. It is not, however, a complete solution as the outcome
 * still depends on the order of file scans and the division of files across
 * readers.
 * <p>
 * Provides the option to infer the schema from the first batch. The "quick path"
 * to obtain the schema will read one batch, then use that schema as the returned
 * schema, returning the full batch in the next call to <tt>next()</tt>.
 */

public class LegacyReaderFactory implements ReaderFactory {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LegacyReaderFactory.class);

  public static class LegacyManagerBuilder {
    protected String scanRootDir;
    protected List<SchemaPath> projection = new ArrayList<>();
    protected MajorType nullType;
    protected boolean useLegacyWildcardExpansion = true;
    protected List<ManagedReader> readers = new ArrayList<>();

    /**
     * Specify the selection root for a directory scan, if any.
     * Used to populate partition columns.
     * @param rootPath Hadoop file path for the directory
     */

    public LegacyManagerBuilder setSelectionRoot(Path rootPath) {
      this.scanRootDir = rootPath.toString();
      return this;
    }

    public LegacyManagerBuilder setSelectionRoot(String rootPath) {
      this.scanRootDir = rootPath;
      return this;
    }

    /**
     * Specify the type to use for projected columns that do not
     * match any data source columns. Defaults to nullable int.
     */

    public LegacyManagerBuilder setNullType(MajorType type) {
      this.nullType = type;
      return this;
    }

    public LegacyManagerBuilder useLegacyWildcardExpansion(boolean flag) {
      useLegacyWildcardExpansion = flag;
      return this;
    }

    @VisibleForTesting
    public LegacyManagerBuilder projectAll() {
      return addProjection(SchemaPath.WILDCARD);
    }

    public LegacyManagerBuilder addProjection(String colName) {
      return addProjection(SchemaPath.getSimplePath(colName));
    }

    public LegacyManagerBuilder addProjection(SchemaPath path) {
      projection.add(path);
      return this;
    }

    public LegacyManagerBuilder addProjection(List<SchemaPath> projection) {
      projection.addAll(projection);
      return this;
    }

    @VisibleForTesting
    public LegacyManagerBuilder setProjection(String[] projection) {
      for (String col : projection) {
        addProjection(col);
      }
      return this;
    }

    public LegacyManagerBuilder addReader(ManagedReader reader) {
      readers.add(reader);
      return this;
    }

    public LegacyReaderFactory build() {
      if (projection.isEmpty()) {
        logger.warn("No projection list specified: assuming SELECT *");
        projectAll();
      }
      return new LegacyReaderFactory(this);
    }
  }

  private ScanProjector scanProjector;
  private final LegacyReaderFactory.LegacyManagerBuilder builder;
  private OperatorContext context;
  private int readerIndex = -1;

  public LegacyReaderFactory(LegacyReaderFactory.LegacyManagerBuilder builder) {
    this.builder = builder;
  }

  public OperatorContext context() { return context; }

  public void publish() {
    scanProjector.publish();
  }

  public VectorContainer output() {
    return scanProjector.output();
  }

  @Override
  public void bind(OperatorContext context) {
    this.context = context;
    ScanProjectionBuilder scanProjBuilder = new ScanProjectionBuilder();
    FileMetadataColumnsParser parser = new FileMetadataColumnsParser(context.getFragmentContext().getOptionSet());
    parser.useLegacyWildcardExpansion(builder.useLegacyWildcardExpansion);
    parser.setScanRootDir(builder.scanRootDir);
    scanProjBuilder.addParser(parser);
    scanProjBuilder.projectedCols(builder.projection);
    ScanLevelProjection scanProj = scanProjBuilder.build();
    FileMetadataProjection metadataPlan = parser.getProjection();
    scanProjector = new ScanProjector(context.getAllocator(), scanProj, metadataPlan, builder.nullType);
  }

  public ResultSetLoader startFile(SchemaNegotiatorImpl schemaNegotiator) {
    scanProjector.startFile(schemaNegotiator.filePath);
    ResultSetLoader tableLoader = scanProjector.makeTableLoader(schemaNegotiator.tableSchema,
        schemaNegotiator.batchSize);
    return tableLoader;
  }

  @Override
  public RowBatchReader nextReader() {
    readerIndex++;
    if (readerIndex >= builder.readers.size()) {
      readerIndex = builder.readers.size();
      return null;
    }
    return new RowBatchReaderShim(this, builder.readers.get(readerIndex));
  }

  @Override
  public void close() {
    if (scanProjector != null) {
      scanProjector.close();
      scanProjector = null;
    }
  }

}