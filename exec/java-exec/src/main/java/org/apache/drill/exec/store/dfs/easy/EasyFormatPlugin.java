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
package org.apache.drill.exec.store.dfs.easy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.AbstractWriter;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.physical.impl.WriterRecordBatch;
import org.apache.drill.exec.physical.impl.protocol.OperatorRecordBatch;
import org.apache.drill.exec.physical.impl.scan.RowBatchReader;
import org.apache.drill.exec.physical.impl.scan.ScanOperatorExec;
import org.apache.drill.exec.physical.impl.scan.file.FileBatchReader;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileScanConfig;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.ColumnExplorer;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.dfs.BasicFormatMatcher;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FormatMatcher;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.schedule.CompleteFileWork;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Base class for various file readers.
 * <p>
 * This version provides a bridge between the legacy {@link RecordReader}-style
 * readers and the newer {@link FileBatchReader} style. Over time, split the
 * class, or provide a cleaner way to handle the differences.
 *
 * @param <T> the format plugin config for this reader
 */

public abstract class EasyFormatPlugin<T extends FormatPluginConfig> implements FormatPlugin {

  public static class EasyFormatConfig {
    public boolean readable = true;
    public boolean writable = false;
    public boolean blockSplittable = false;
    public boolean compressible = false;
    public List<String> extensions;
    public String defaultName;
    public ScannerVersion scannerVersion = ScannerVersion.ORIGINAL;
  }

  protected enum ScannerVersion {

    /**
     * Use the original scanner based on the
     * {@link RecordReader} interface. Requires that the storage
     * plugin roll its own solutions for null columns. Is not able
     * to limit vector or batch sizes. Retained or backward compatibility
     * with Drill 1.11 and earlier format plugins.
     */
    ORIGINAL,

    /**
     * Revised scanner based on the revised
     * {@link ResultSetLoader} and {@link RowBatchReader} classes.
     * Handles most projection tasks automatically. Able to limit
     * vector and batch sizes. Use this for new format plugins.
     */
    ROW_SET_LOADER
  }

  private final BasicFormatMatcher matcher;
  private final DrillbitContext context;
  private final boolean readable;
  private final boolean writable;
  private final boolean blockSplittable;
  private final Configuration fsConf;
  private final StoragePluginConfig storageConfig;
  protected final T formatConfig;
  private final String name;
  private final boolean compressible;
  private final ScannerVersion usesScannerVersion;

  protected EasyFormatPlugin(String name, DrillbitContext context, Configuration fsConf,
      StoragePluginConfig storageConfig, T formatConfig, boolean readable, boolean writable,
      boolean blockSplittable,
      boolean compressible, List<String> extensions, String defaultName) {
    this(name, context, fsConf, storageConfig, formatConfig, readable, writable, blockSplittable,
        compressible, extensions, defaultName, ScannerVersion.ORIGINAL);
  }

  protected EasyFormatPlugin(String name, DrillbitContext context, Configuration fsConf,
      StoragePluginConfig storageConfig, T formatConfig, EasyFormatConfig config) {
    this(name, context, fsConf,
        storageConfig, formatConfig,
        config.readable, config.writable,
        config.blockSplittable,
        config.compressible, config.extensions, config.defaultName,
        config.scannerVersion);
  }

  protected EasyFormatPlugin(String name, DrillbitContext context, Configuration fsConf,
      StoragePluginConfig storageConfig, T formatConfig, boolean readable, boolean writable,
      boolean blockSplittable,
      boolean compressible, List<String> extensions, String defaultName,
      ScannerVersion scannerVersion) {
    this.matcher = new BasicFormatMatcher(this, fsConf, extensions, compressible);
    this.readable = readable;
    this.writable = writable;
    this.context = context;
    this.blockSplittable = blockSplittable;
    this.compressible = compressible;
    this.fsConf = fsConf;
    this.storageConfig = storageConfig;
    this.formatConfig = formatConfig;
    this.name = name == null ? defaultName : name;
    this.usesScannerVersion = scannerVersion;
  }

  @Override
  public Configuration getFsConf() { return fsConf; }

  @Override
  public DrillbitContext getContext() { return context; }

  @Override
  public String getName() { return name; }

  public abstract boolean supportsPushDown();

  /**
   * Whether or not you can split the format based on blocks within file
   * boundaries. If not, the simple format engine will only split on file
   * boundaries.
   *
   * @return <code>true</code> if splittable.
   */
  public boolean isBlockSplittable() {
    return blockSplittable;
  }

  /**
   * Indicates whether or not this format could also be in a compression
   * container (for example: csv.gz versus csv). If this format uses its own
   * internal compression scheme, such as Parquet does, then this should return
   * false.
   *
   * @return <code>true</code> if it is compressible
   */
  public boolean isCompressible() {
    return compressible;
  }

  /**
   * Return a record reader for the specific file format, when using the original
   * {@link ScanBatch} scanner.
   * @param context fragment context
   * @param dfs Drill file system
   * @param fileWork metadata about the file to be scanned
   * @param columns list of projected columns (or may just contain the wildcard)
   * @param userName the name of the user running the query
   * @return a record reader for this format
   * @throws ExecutionSetupException for many reasons
   */

  public RecordReader getRecordReader(FragmentContext context, DrillFileSystem dfs, FileWork fileWork,
      List<SchemaPath> columns, String userName) throws ExecutionSetupException {
    throw new ExecutionSetupException("Must implement getRecordReader() if using the legacy scanner.");
  }

  protected CloseableRecordBatch getReaderBatch(final FragmentContext context,
      final EasySubScan scan) throws ExecutionSetupException {
    switch (usesScannerVersion) {
    case ORIGINAL:
      return buildOriginalScan(context, scan);
    case ROW_SET_LOADER:
      return buildRevisedScan(context, scan);
    default:
      throw new IllegalStateException( "Unexpected scanner version: " + usesScannerVersion);
    }
  }

  @SuppressWarnings("resource")
  private CloseableRecordBatch buildOriginalScan(final FragmentContext context, EasySubScan scan) throws ExecutionSetupException {
    final ColumnExplorer columnExplorer = new ColumnExplorer(context, scan.getColumns());

    if (! columnExplorer.isStarQuery()) {
      scan = new EasySubScan(scan.getUserName(), scan.getWorkUnits(), scan.getFormatPlugin(),
          columnExplorer.getTableColumns(), scan.getSelectionRoot());
      scan.setOperatorId(scan.getOperatorId());
    }

    OperatorContext oContext = context.newOperatorContext(scan);
    final DrillFileSystem dfs;
    try {
      dfs = oContext.newFileSystem(fsConf);
    } catch (IOException e) {
      throw new ExecutionSetupException(String.format("Failed to create FileSystem: %s", e.getMessage()), e);
    }

    List<RecordReader> readers = Lists.newArrayList();
    List<Map<String, String>> implicitColumns = Lists.newArrayList();
    Map<String, String> mapWithMaxColumns = Maps.newLinkedHashMap();
    for(FileWork work : scan.getWorkUnits()){
      RecordReader recordReader = getRecordReader(context, dfs, work, scan.getColumns(), scan.getUserName());
      readers.add(recordReader);
      Map<String, String> implicitValues = columnExplorer.populateImplicitColumns(work, scan.getSelectionRoot());
      implicitColumns.add(implicitValues);
      if (implicitValues.size() > mapWithMaxColumns.size()) {
        mapWithMaxColumns = implicitValues;
        }
      }

    // all readers should have the same number of implicit columns, add missing ones with value null
    Map<String, String> diff = Maps.transformValues(mapWithMaxColumns, Functions.constant((String) null));
    for (Map<String, String> map : implicitColumns) {
      map.putAll(Maps.difference(map, diff).entriesOnlyOnRight());
    }

    return new ScanBatch(scan, context, oContext, readers, implicitColumns);
  }

  public FileBatchReader makeBatchReader(DrillFileSystem dfs, FileSplit split) throws ExecutionSetupException {
    throw new ExecutionSetupException("Must implement getRecordReader() if using the revised scanner.");
  }

  /**
   * Builds the revised {@link FileBatchReader}-based scan batch.
   *
   * @param context
   * @param scan
   * @return
   * @throws ExecutionSetupException
   */

  @SuppressWarnings("resource")
  private CloseableRecordBatch buildRevisedScan(final FragmentContext context, EasySubScan scan) throws ExecutionSetupException {

    // Create the operator-specific context

    OperatorContext oContext = context.newOperatorContext(scan);

    // And the Drill file system.
    // TODO: Move this into the scan framework, made available via
    // the schema negotiator. No reason for this to be in the context as
    // not all scanner need a file system.

    final DrillFileSystem dfs;
    try {
      dfs = oContext.newFileSystem(fsConf);
    } catch (IOException e) {
      throw new ExecutionSetupException(String.format("Failed to create FileSystem: %s", e.getMessage()), e);
    }

    // Configure the file scan.

    FileScanConfig scanConfig = new FileScanConfig();
    scanConfig.setProjection(scan.getColumns());
    scanConfig.setSelectionRoot(new Path(scan.getSelectionRoot()));
    scanConfig.useLegacyWildcardExpansion(true);
//        .setUserName(scan.getUserName());

    // Create the list of readers and gather the list of file names.
    // TODO: Create the readers dynamically rather than up front.

    List<FileBatchReader> readers = new ArrayList<>();
    for(FileWork work : scan.getWorkUnits()) {
      Path path = dfs.makeQualified(new Path(work.getPath()));
      FileSplit split = new FileSplit(path, work.getStart(), work.getLength(), new String[]{""});
      scanConfig.addFile(path);
      readers.add(makeBatchReader(dfs, split));
    }
    scanConfig.setReaderFactory(readers.iterator());

    // Assemble the scan operator and its wrapper.

    try {
      return new OperatorRecordBatch(
          context, scan,
          new ScanOperatorExec(
              new FileScanFramework(scanConfig)));
    } catch (UserException e) {
      throw e;
    } catch (Throwable e) {
      throw new ExecutionSetupException(e);
    }
  }

  public abstract RecordWriter getRecordWriter(FragmentContext context, EasyWriter writer) throws IOException;

  public CloseableRecordBatch getWriterBatch(FragmentContext context, RecordBatch incoming, EasyWriter writer)
      throws ExecutionSetupException {
    try {
      return new WriterRecordBatch(writer, incoming, context, getRecordWriter(context, writer));
    } catch(IOException e) {
      throw new ExecutionSetupException(String.format("Failed to create the WriterRecordBatch. %s", e.getMessage()), e);
    }
  }

  protected ScanStats getScanStats(final PlannerSettings settings, final EasyGroupScan scan) {
    long data = 0;
    for (final CompleteFileWork work : scan.getWorkIterable()) {
      data += work.getTotalBytes();
    }

    final long estRowCount = data / 1024;
    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, estRowCount, 1, data);
  }

  @Override
  public AbstractWriter getWriter(PhysicalOperator child, String location, List<String> partitionColumns) throws IOException {
    return new EasyWriter(child, location, partitionColumns, this);
  }

  @Override
  public AbstractGroupScan getGroupScan(String userName, FileSelection selection, List<SchemaPath> columns)
      throws IOException {
    return new EasyGroupScan(userName, selection, this, columns, selection.selectionRoot);
  }

  @Override
  public T getConfig() {
    return formatConfig;
  }

  @Override
  public StoragePluginConfig getStorageConfig() {
    return storageConfig;
  }

  @Override
  public boolean supportsRead() {
    return readable;
  }

  @Override
  public boolean supportsWrite() {
    return writable;
  }

  @Override
  public boolean supportsAutoPartitioning() {
    return false;
  }

  @Override
  public FormatMatcher getMatcher() {
    return matcher;
  }

  @Override
  public Set<StoragePluginOptimizerRule> getOptimizerRules() {
    return ImmutableSet.of();
  }

  public abstract int getReaderOperatorType();
  public abstract int getWriterOperatorType();

}
