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

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.scan.ScanSchema;
import org.apache.drill.exec.physical.impl.scan.SchemaNegotiatorImpl;
import org.apache.drill.exec.physical.impl.scan.metadata.FileMetadataColumnsParser;
import org.apache.drill.exec.physical.impl.scan.metadata.FileMetadataColumnsParser.FileMetadataProjection;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;

public class Exp {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Exp.class);

  public static class ProjectionLifecycle2 {

    public void startScan() { }
    public void startReader() { }
    public void startSchema() { }
    public void endReader() { }
    public void endScan() { }
  }

  public interface SchemaManager extends ScanSchema {
    void bind(VectorContainer container);
    @Override
    void publish();
  }

  public static class LegacyManagerBuilder {
    protected String scanRootDir;
    protected List<SchemaPath> projection = new ArrayList<>();
    protected MajorType nullType;
    protected boolean useLegacyWildcardExpansion = true;

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

    public LegacyManager build() {
      if (projection.isEmpty()) {
        logger.warn("No projection list specified: assuming SELECT *");
        projectAll();
      }
      return new LegacyManager(this);
    }
  }

  public static class LegacyManager implements SchemaManager {

    private VectorContainer outputContainer;
    private ScanProjector scanProjector;
    private final LegacyManagerBuilder builder;
    private ResultSetLoader tableLoader;
    private boolean earlySchema;
    private OperatorContext context;

    public LegacyManager(LegacyManagerBuilder builder) {
      this.builder = builder;
    }

    @Override
    public void bind(VectorContainer container) {
      outputContainer = container;
    }

    public OperatorContext context() { return context; }

    @Override
    public void publish() {
      scanProjector.publish();
    }

    @Override
    public void build(OperatorContext context) {
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

    @Override
    public void close() {
      if (scanProjector != null) {
        scanProjector.close();
        scanProjector = null;
      }
    }

    public ResultSetLoader buildSchema(SchemaNegotiatorImpl schemaNegotiator) {
      earlySchema = schemaNegotiator.tableSchema != null;

      scanProjector.startFile(schemaNegotiator.filePath);
      tableLoader = scanProjector.makeTableLoader(schemaNegotiator.tableSchema,
          schemaNegotiator.batchSize);
      return tableLoader;
    }

    @Override
    public boolean isEarlySchema() { return earlySchema; }

    @Override
    public VectorContainer output() {
      return scanProjector.output();
    }

    public SchemaNegotiatorImpl newReader() {
      return new SchemaNegotiatorImpl(context(), this);
    }
  }

//  public static class BaseSchemaManager implements SchemaManager {
//
//    @Override
//    public void bind(VectorContainer container) {
//      // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public void publish() {
//      // TODO Auto-generated method stub
//
//    }
//
//  }
//
//  public static class FileSchemaManager extends BaseSchemaManager {
//
//  }
//
//  public static class TextFileSchemaManager extends FileSchemaManager {
//
//  }
}
