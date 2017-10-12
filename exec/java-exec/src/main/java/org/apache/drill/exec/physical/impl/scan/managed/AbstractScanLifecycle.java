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
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanProjectionBuilder;
import org.apache.drill.exec.physical.impl.scan.project.ScanProjector;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.record.VectorContainer;

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

public abstract class AbstractScanLifecycle implements ReaderFactory {

  /**
   * Extensible configuration for a scan.
   * <p>
   * Note: does not use the fluent style because many
   * subclasses exist.
   */

  public static class BasicScanConfig {
    protected List<SchemaPath> projection = new ArrayList<>();
    protected MajorType nullType;
    protected List<ManagedReader> readers = new ArrayList<>();

    /**
     * Specify the type to use for projected columns that do not
     * match any data source columns. Defaults to nullable int.
     */

    public void setNullType(MajorType type) {
      this.nullType = type;
    }

    public void setProjection(List<SchemaPath> projection) {
      this.projection = projection;
    }

    public void addReader(ManagedReader reader) {
      readers.add(reader);
    }
  }

  protected ScanProjector scanProjector;
  protected OperatorContext context;
  private int readerIndex = -1;

  @Override
  public void bind(OperatorContext context) {
    this.context = context;
    ScanProjectionBuilder scanProjBuilder = new ScanProjectionBuilder();
    AbstractScanLifecycle.BasicScanConfig scanConfig = scanConfig();
    scanProjBuilder.projectedCols(scanConfig.projection);
    defineParsers(scanProjBuilder);
    ScanLevelProjection scanProj = scanProjBuilder.build();
    buildProjector(scanProj);
  }

  protected abstract AbstractScanLifecycle.BasicScanConfig scanConfig();

  protected void defineParsers(ScanProjectionBuilder scanProjBuilder) { }

  protected abstract void buildProjector(ScanLevelProjection scanProj);

  @Override
  public RowBatchReader nextReader() {
    AbstractScanLifecycle.BasicScanConfig scanConfig = scanConfig();
    readerIndex++;
    if (readerIndex >= scanConfig.readers.size()) {
      readerIndex = scanConfig.readers.size();
      return null;
    }
    return new RowBatchReaderShim(this, scanConfig.readers.get(readerIndex));
  }

  public OperatorContext context() { return context; }

  public ResultSetLoader startFile(SchemaNegotiatorImpl schemaNegotiator) {
    scanProjector.startFile(schemaNegotiator.filePath);
    ResultSetLoader tableLoader = scanProjector.makeTableLoader(schemaNegotiator.tableSchema,
        schemaNegotiator.batchSize);
    return tableLoader;
  }

  public void publish() {
    scanProjector.publish();
  }

  public VectorContainer output() {
    return scanProjector.output();
  }

  @Override
  public void close() {
    if (scanProjector != null) {
      scanProjector.close();
      scanProjector = null;
    }
  }
}
