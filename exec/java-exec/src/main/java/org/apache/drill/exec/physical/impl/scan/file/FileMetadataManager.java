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
package org.apache.drill.exec.physical.impl.scan.file;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.directory.api.util.Strings;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.impl.scan.project.ConstantColumnLoader;
import org.apache.drill.exec.physical.impl.scan.project.ConstantColumnLoader.ConstantColumnSpec;
import org.apache.drill.exec.physical.impl.scan.project.MetadataManager;
import org.apache.drill.exec.physical.impl.scan.project.ReaderLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.RowBatchMerger.Projection;
import org.apache.drill.exec.physical.impl.scan.project.RowBatchMerger.VectorSource;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ColumnProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ScanProjectionParser;
import org.apache.drill.exec.physical.impl.scan.project.SchemaLevelProjection.ResolvedColumn;
import org.apache.drill.exec.physical.impl.scan.project.SchemaLevelProjection.SchemaProjectionResolver;
import org.apache.drill.exec.physical.rowSet.ResultVectorCache;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.store.ColumnExplorer.ImplicitFileColumns;
import org.apache.hadoop.fs.Path;

public class FileMetadataManager implements MetadataManager, SchemaProjectionResolver, VectorSource {

  public static abstract class MetadataColumn implements ResolvedColumn, ConstantColumnSpec, ColumnProjection {

    public final MaterializedField schema;
    public final Projection projection;
    public final String value;

    public MetadataColumn(String name, MajorType type, String value, Projection projection) {
      schema = MaterializedField.create(name, type);
      this.projection = projection;
      this.value = value;
    }

    @Override
    public MaterializedField schema() { return schema; }

    @Override
    public String value() { return value; }

    @Override
    public String name() { return schema.getName(); }

    @Override
    public Projection projection() { return projection; }

    public abstract MetadataColumn resolve(FileMetadata fileInfo);

    public abstract MetadataColumn project(Projection projection);
  }

  public static class FileMetadataColumn extends MetadataColumn {

    public static final int ID = 15;

    private final FileMetadataColumnDefn defn;

    /**
     * Constructor for resolved column.
     *
     * @param name
     * @param defn
     * @param fileInfo
     * @param projection
     */
    public FileMetadataColumn(String name, FileMetadataColumnDefn defn,
        FileMetadata fileInfo) {
      super(name, defn.dataType(), defn.defn.getValue(fileInfo.filePath()), null);
      this.defn = defn;
    }

    /**
     * Constructor for unresolved column.
     *
     * @param name
     * @param defn
     */

    public FileMetadataColumn(String name, FileMetadataColumnDefn defn) {
      super(name, defn.dataType(), null, null);
      this.defn = defn;
    }

    public FileMetadataColumn(String name, FileMetadataColumnDefn defn,
        String value, Projection projection) {
      super(name, defn.dataType(), value, projection);
      this.defn = defn;
    }

    @Override
    public int nodeType() { return ID; }

    public FileMetadataColumnDefn defn() { return defn; }

    @Override
    public MetadataColumn resolve(FileMetadata fileInfo) {
      return new FileMetadataColumn(name(), defn, fileInfo);
    }

    @Override
    public MetadataColumn project(Projection projection) {
      return new FileMetadataColumn(name(), defn, value(), projection);
    }
  }

  public static class PartitionColumn extends MetadataColumn {

    public static final int ID = 16;

    protected final int partition;

    public PartitionColumn(String name, int partition,
        FileMetadata fileInfo) {
      super(name, dataType(), fileInfo.partition(partition), null);
      this.partition = partition;
    }

    public PartitionColumn(String name, int partition) {
      super(name, dataType(), null, null);
      this.partition = partition;
    }

    public PartitionColumn(String name, int partition,
        String value, Projection projection) {
      super(name, dataType(), value, projection);
      this.partition = partition;
    }

    public int partition() { return partition; }

    @Override
    public int nodeType() { return ID; }

    @Override
    public MetadataColumn resolve(FileMetadata fileInfo) {
      return new PartitionColumn(name(), partition, fileInfo);
    }

    @Override
    public MetadataColumn project(Projection projection) {
      return new PartitionColumn(name(), partition, value(), projection);
    }

    public static MajorType dataType() {
      return MajorType.newBuilder()
            .setMinorType(MinorType.VARCHAR)
            .setMode(DataMode.OPTIONAL)
            .build();
    }
  }

  // Input

  private Path scanRootDir;
  private FileMetadata currentFile;

  // Config

  protected final String partitionDesignator;
  protected List<FileMetadataColumnDefn> implicitColDefns = new ArrayList<>();;
  protected Map<String, FileMetadataColumnDefn> fileMetadataColIndex = CaseInsensitiveMap.newHashMap();
  protected boolean useLegacyWildcardExpansion = true;
  private final FileMetadataColumnsParser parser;

  // Internal state

  private ResultVectorCache vectorCache;
  private List<MetadataColumn> metadataColumns = new ArrayList<>();
  private ConstantColumnLoader loader;
  private VectorContainer outputContainer;

  /**
   * Specifies whether to plan based on the legacy meaning of "*". See
   * <a href="https://issues.apache.org/jira/browse/DRILL-5542">DRILL-5542</a>.
   * If true, then the star column <i>includes</i> implicit and partition
   * columns. If false, then star matches <i>only</i> table columns.
   * @param flag true to use the legacy plan, false to use the revised
   * semantics
   * @return this builder
   */

  public FileMetadataManager(OptionSet optionManager,
      boolean useLegacyWildcardExpansion,
      Path rootDir) {
    this.useLegacyWildcardExpansion = useLegacyWildcardExpansion;
    scanRootDir = rootDir;

    partitionDesignator = optionManager.getString(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL);
    for (ImplicitFileColumns e : ImplicitFileColumns.values()) {
      String colName = optionManager.getString(e.optionName());
      if (! Strings.isEmpty(colName)) {
        FileMetadataColumnDefn defn = new FileMetadataColumnDefn(colName, e);
        implicitColDefns.add(defn);
        fileMetadataColIndex.put(defn.colName, defn);
      }
    }
    parser = new FileMetadataColumnsParser(this);
  }

  @Override
  public void bind(ResultVectorCache vectorCache) {
    this.vectorCache = vectorCache;
  }

  @Override
  public ScanProjectionParser projectionParser() {
    return parser;
  }

  public FileMetadata fileMetadata(Path filePath) {
    return new FileMetadata(filePath, scanRootDir);
  }

  public boolean useLegacyWildcardPartition() { return useLegacyWildcardExpansion; }

  public boolean hasMetadata() { return parser.hasMetadata; }

  public String partitionName(int partition) {
    return partitionDesignator + partition;
  }

  public List<FileMetadataColumnDefn> fileMetadataColDefns() { return implicitColDefns; }

  public void startFile(Path filePath) {
    currentFile = fileMetadata(filePath);
  }

  @Override
  public ReaderLevelProjection resolve(ScanLevelProjection scanProj) {
    if (currentFile == null) {
      throw new IllegalStateException("Must start the file before doing table-level resolution");
    }
    return new FileLevelProjection(scanProj, this, currentFile);
  }

  @Override
  public SchemaProjectionResolver resolver() {
    return this;
  }

  @Override
  public void define() {
    assert loader == null;
    if (metadataColumns.isEmpty()) {
      return;
    }
    loader = new ConstantColumnLoader(vectorCache, metadataColumns);
  }

  @Override
  public void load(int rowCount) {
    if (loader == null) {
      return;
    }
    outputContainer = loader.load(rowCount);
  }

  @Override
  public void close() {
    reset();
  }

  @Override
  public void reset() {
    metadataColumns.clear();
    if (loader != null) {
      loader.close();
      loader = null;
    }
  }

  @Override
  public void endFile() {
    reset();
    currentFile = null;
  }

  @Override
  public boolean resolveColumn(ColumnProjection col, List<ResolvedColumn> output) {
    switch (col.nodeType()) {
    case FileMetadataColumn.ID:
    case PartitionColumn.ID:
      MetadataColumn projectedColumn = ((MetadataColumn) col).project(
          new Projection(this, true, metadataColumns.size(), output.size()));
      metadataColumns.add(projectedColumn);
      output.add(projectedColumn);
      return true;
    default:
      return false;
    }
  }

  @Override
  public VectorContainer container() {
    return outputContainer;
  }
}
