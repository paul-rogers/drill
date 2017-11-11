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
package org.apache.drill.exec.store.easy.json;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsScanFramework.ColumnsSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.file.BaseFileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileReaderCreator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.drill.exec.store.dfs.easy.EasyWriter;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin.EasyFormatConfig;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin.ScanBatchCreator;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin.ScanFrameworkCreator;
import org.apache.drill.exec.store.easy.json.JSONFormatPlugin.JSONFormatConfig;
import org.apache.drill.exec.store.easy.text.TextFormatPlugin;
import org.apache.drill.exec.store.easy.text.TextFormatPlugin.TextFormatConfig;
import org.apache.drill.exec.store.easy.text.TextFormatPlugin.TextScanBatchCreator;
import org.apache.drill.exec.store.easy.text.compliant.CompliantTextBatchReader;
import org.apache.drill.exec.store.easy.text.compliant.TextParsingSettings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.FileSplit;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

public class JSONFormatPlugin extends EasyFormatPlugin<JSONFormatConfig>
    implements FileReaderCreator {

  private static final boolean IS_COMPRESSIBLE = true;
  private static final String PLUGIN_NAME = "json";
  private static final String DEFAULT_EXTN = "json";

  @JsonTypeName(PLUGIN_NAME)
  public static class JSONFormatConfig implements FormatPluginConfig {

    public List<String> extensions = ImmutableList.of(DEFAULT_EXTN);
    private static final List<String> DEFAULT_EXTS = ImmutableList.of(DEFAULT_EXTN);

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public List<String> getExtensions() {
      if (extensions == null) {
        // when loading an old JSONFormatConfig that doesn't contain an "extensions" attribute
        return DEFAULT_EXTS;
      }
      return extensions;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((extensions == null) ? 0 : extensions.hashCode());
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
      JSONFormatConfig other = (JSONFormatConfig) obj;
      if (extensions == null) {
        if (other.extensions != null) {
          return false;
        }
      } else if (!extensions.equals(other.extensions)) {
        return false;
      }
      return true;
    }
  }

  public static class JsonScanBatchCreator extends ScanFrameworkCreator {

    private final FileReaderCreator readerCreator;
    @SuppressWarnings("unused")
    private final JSONFormatPlugin jsonPlugin;

    public JsonScanBatchCreator(JSONFormatPlugin plugin,
        FileReaderCreator readerCreator) {
      super(plugin);
      this.readerCreator = readerCreator;
      jsonPlugin = plugin;
    }

    @Override
    protected FileScanFramework buildFramework(
        EasySubScan scan) throws ExecutionSetupException {
      FileScanFramework framework = new FileScanFramework(
              scan.getColumns(),
              scan.getWorkUnits(),
              plugin.easyConfig().fsConf,
              readerCreator);

      // For now, maintain backward compatibility with metadata
      // position in wildcard queries.

      framework.useDrill1_12MetadataPosition(true);

      return framework;
    }
  }

  public JSONFormatPlugin(String name, DrillbitContext context, Configuration fsConf, StoragePluginConfig storageConfig) {
    this(name, context, fsConf, storageConfig, new JSONFormatConfig());
  }

  public JSONFormatPlugin(String name, DrillbitContext context, Configuration fsConf,
      StoragePluginConfig config, JSONFormatConfig formatPluginConfig) {
    super(name, easyConfig(fsConf, formatPluginConfig), context, config, formatPluginConfig);
  }

  private static EasyFormatConfig easyConfig(Configuration fsConf, JSONFormatConfig pluginConfig) {
    EasyFormatConfig config = new EasyFormatConfig();
    config.readable = true;
    config.writable = false;
    config.blockSplittable = false;
    config.compressible = IS_COMPRESSIBLE;
    config.supportsProjectPushdown = true;
    config.extensions = pluginConfig.getExtensions();
    config.fsConf = fsConf;
    config.defaultName = PLUGIN_NAME;
    return config;
  }

  @Override
  protected ScanBatchCreator scanBatchCreator() {
    return new JsonScanBatchCreator(this, this);
  }

  @Override
  public RecordWriter getRecordWriter(FragmentContext context, EasyWriter writer) throws IOException {
    Map<String, String> options = Maps.newHashMap();

    options.put("location", writer.getLocation());

    FragmentHandle handle = context.getHandle();
    String fragmentId = String.format("%d_%d", handle.getMajorFragmentId(), handle.getMinorFragmentId());
    options.put("prefix", fragmentId);

    options.put("separator", " ");
    options.put(FileSystem.FS_DEFAULT_NAME_KEY, ((FileSystemConfig)writer.getStorageConfig()).connection);

    options.put("extension", "json");
    options.put("extended", Boolean.toString(context.getOptions().getOption(ExecConstants.JSON_EXTENDED_TYPES)));
    options.put("uglify", Boolean.toString(context.getOptions().getOption(ExecConstants.JSON_WRITER_UGLIFY)));
    options.put("skipnulls", Boolean.toString(context.getOptions().getOption(ExecConstants.JSON_WRITER_SKIPNULLFIELDS)));

    RecordWriter recordWriter = new JsonRecordWriter(writer.getStorageStrategy());
    recordWriter.init(options);

    return recordWriter;
  }
  @Override
  public int getReaderOperatorType() {
    return CoreOperatorType.JSON_SUB_SCAN_VALUE;
  }

  @Override
  public int getWriterOperatorType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ManagedReader<FileSchemaNegotiator> makeBatchReader(
      DrillFileSystem dfs,
      FileSplit split) throws ExecutionSetupException {
    return new JsonBatchReader(split, dfs);
  }
}
