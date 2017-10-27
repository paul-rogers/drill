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
package org.apache.drill.exec.store.easy.text;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsScanFramework;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsScanFramework.ColumnsSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsScanFramework.FileReaderCreator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasyGroupScan;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.drill.exec.store.dfs.easy.EasyWriter;
import org.apache.drill.exec.store.easy.text.compliant.CompliantTextBatchReader;
import org.apache.drill.exec.store.easy.text.compliant.TextParsingSettings;
import org.apache.drill.exec.store.schedule.CompleteFileWork;
import org.apache.drill.exec.store.text.DrillTextRecordWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

public class TextFormatPlugin extends EasyFormatPlugin<TextFormatPlugin.TextFormatConfig>
      implements FileReaderCreator {
  private final static String DEFAULT_NAME = "text";

  public static class TextScanBatchCreator
        extends ScanFrameworkCreator<ColumnsSchemaNegotiator> {

    private final FileReaderCreator readerCreator;

    public TextScanBatchCreator(EasyFormatPlugin<? extends FormatPluginConfig> plugin,
        FileReaderCreator readerCreator) {
      super(plugin);
      this.readerCreator = readerCreator;
    }

    @Override
    protected ColumnsScanFramework buildFramework(
        EasySubScan scan) throws ExecutionSetupException {
      ColumnsScanFramework framework = new ColumnsScanFramework(
              scan.getColumns(),
              scan.getWorkUnits(),
              plugin.easyConfig().fsConf,
              readerCreator);
      framework.setSelectionRoot(new Path(scan.getSelectionRoot()));
      framework.useLegacyWildcardExpansion(true);
      return framework;
    }
  }

  public TextFormatPlugin(String name, DrillbitContext context, Configuration fsConf, StoragePluginConfig storageConfig) {
     this(name, context, fsConf, storageConfig, new TextFormatConfig());
  }

  public TextFormatPlugin(String name, DrillbitContext context, Configuration fsConf, StoragePluginConfig config,
      TextFormatConfig formatPluginConfig) {
    super(name, easyConfig(fsConf), context, config, formatPluginConfig);
  }

  private static EasyFormatConfig easyConfig(Configuration fsConf) {
    EasyFormatConfig config = new EasyFormatConfig();
    config.readable = true;
    config.writable = false;
    config.blockSplittable = true;
    config.compressible = true;
    config.extensions = Collections.<String>emptyList();
    config.fsConf = fsConf;
    config.defaultName = DEFAULT_NAME;
    return config;
  }

  @Override
  protected ScanBatchCreator scanBatchCreator() {
    return new TextScanBatchCreator(this, this);
  }

  @Override
  public AbstractGroupScan getGroupScan(String userName, FileSelection selection, List<SchemaPath> columns)
      throws IOException {
    return new EasyGroupScan(userName, selection, this, columns, selection.selectionRoot);
  }

  @Override
  protected ScanStats getScanStats(final PlannerSettings settings, final EasyGroupScan scan) {
    long data = 0;
    for (final CompleteFileWork work : scan.getWorkIterable()) {
      data += work.getTotalBytes();
    }
    final double estimatedRowSize = settings.getOptions().getOption(ExecConstants.TEXT_ESTIMATED_ROW_SIZE);
    final double estRowCount = data / estimatedRowSize;
    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, (long) estRowCount, 1, data);
  }

  @Override
  public RecordWriter getRecordWriter(final FragmentContext context, final EasyWriter writer) throws IOException {
    final Map<String, String> options = Maps.newHashMap();

    options.put("location", writer.getLocation());

    FragmentHandle handle = context.getHandle();
    String fragmentId = String.format("%d_%d", handle.getMajorFragmentId(), handle.getMinorFragmentId());
    options.put("prefix", fragmentId);

    options.put("separator", ((TextFormatConfig)getConfig()).getFieldDelimiterAsString());
    options.put(FileSystem.FS_DEFAULT_NAME_KEY, ((FileSystemConfig)writer.getStorageConfig()).connection);

    options.put("extension", ((TextFormatConfig)getConfig()).getExtensions().get(0));

    RecordWriter recordWriter = new DrillTextRecordWriter(context.getAllocator(), writer.getStorageStrategy());
    recordWriter.init(options);

    return recordWriter;
  }

  @JsonTypeName("text") @JsonInclude(Include.NON_DEFAULT)
  public static class TextFormatConfig implements FormatPluginConfig {

    public List<String> extensions = ImmutableList.of();
    public String lineDelimiter = "\n";
    public char fieldDelimiter = '\n';
    public char quote = '"';
    public char escape = '"';
    public char comment = '#';
    public boolean skipFirstLine = false;
    public boolean extractHeader = false;

    public List<String> getExtensions() { return extensions; }

    public char getQuote() { return quote; }

    public char getEscape() { return escape; }

    public char getComment() { return comment; }

    public String getLineDelimiter() { return lineDelimiter; }

    public char getFieldDelimiter() { return fieldDelimiter; }

    @JsonIgnore
    public boolean isHeaderExtractionEnabled() {
      return extractHeader;
    }

    @JsonIgnore
    public String getFieldDelimiterAsString(){
      return new String(new char[]{fieldDelimiter});
    }

    @Deprecated
    @JsonProperty("delimiter")
    public void setFieldDelimiter(char delimiter){
      this.fieldDelimiter = delimiter;
    }

    public boolean isSkipFirstLine() { return skipFirstLine; }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + comment;
      result = prime * result + escape;
      result = prime * result + ((extensions == null) ? 0 : extensions.hashCode());
      result = prime * result + fieldDelimiter;
      result = prime * result + ((lineDelimiter == null) ? 0 : lineDelimiter.hashCode());
      result = prime * result + quote;
      result = prime * result + (skipFirstLine ? 1231 : 1237);
      result = prime * result + (extractHeader ? 1231 : 1237);
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      TextFormatConfig other = (TextFormatConfig) obj;
      if (comment != other.comment) {
        return false;
      }
      if (escape != other.escape) {
        return false;
      }
      if (extensions == null) {
        if (other.extensions != null) {
          return false;
        }
      } else if (!extensions.equals(other.extensions)) {
        return false;
      }
      if (fieldDelimiter != other.fieldDelimiter) {
        return false;
      }
      if (lineDelimiter == null) {
        if (other.lineDelimiter != null) {
          return false;
        }
      } else if (!lineDelimiter.equals(other.lineDelimiter)) {
        return false;
      }
      if (quote != other.quote) {
        return false;
      }
      if (skipFirstLine != other.skipFirstLine) {
        return false;
      }
      if (extractHeader != other.extractHeader) {
        return false;
      }
      return true;
    }
  }

  @Override
  public int getReaderOperatorType() {
    return CoreOperatorType.TEXT_SUB_SCAN_VALUE;
  }

  @Override
  public int getWriterOperatorType() {
    return CoreOperatorType.TEXT_WRITER_VALUE;
  }

  @Override
  public boolean supportsPushDown() { return true; }

  @Override
  public ManagedReader<ColumnsSchemaNegotiator> makeBatchReader(
      DrillFileSystem dfs,
      FileSplit split) throws ExecutionSetupException {
    TextParsingSettings settings = new TextParsingSettings();
    settings.set((TextFormatConfig) getConfig());
    return new CompliantTextBatchReader(split, dfs, settings);
  }
}
