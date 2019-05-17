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

package org.apache.drill.exec.store.log;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileReaderFactory;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileScanBuilder;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;

public class LogFormatPlugin extends EasyFormatPlugin<LogFormatConfig> {

  public static final String PLUGIN_NAME = "logRegex";

  private static class LogScanBatchCreator extends ScanFrameworkCreator {

    private final LogFormatPlugin logPlugin;

    public LogScanBatchCreator(LogFormatPlugin plugin) {
      super(plugin);
      logPlugin = plugin;
    }

    @Override
    protected FileScanBuilder frameworkBuilder(
        EasySubScan scan) throws ExecutionSetupException {
      FileScanBuilder builder = new FileScanBuilder();
      builder.setReaderFactory(new LogReaderFactory(logPlugin));

      // The default type of regex columns is nullable VarChar,
      // so let's use that as the missing column type.

      builder.setNullType(Types.optional(MinorType.VARCHAR));

      // Pass along the output schema, if any

      builder.setOutputSchema(scan.getSchema());
      return builder;
    }
  }

  private static class LogReaderFactory extends FileReaderFactory {

    private final LogFormatPlugin plugin;

    public LogReaderFactory(LogFormatPlugin plugin) {
      this.plugin = plugin;
    }

    @Override
    public ManagedReader<? extends FileSchemaNegotiator> newReader(
        FileSplit split) {
       return new LogBatchReader(split, plugin.getConfig());
    }

  }

  public LogFormatPlugin(String name, DrillbitContext context,
                         Configuration fsConf, StoragePluginConfig storageConfig,
                         LogFormatConfig formatConfig) {
    super(name, easyConfig(fsConf, formatConfig), context, storageConfig, formatConfig);
  }

  private static EasyFormatConfig easyConfig(Configuration fsConf, LogFormatConfig pluginConfig) {
    EasyFormatConfig config = new EasyFormatConfig();
    config.readable = true;
    config.writable = false;
    // Should be block splittable, but logic not yet implemented.
    config.blockSplittable = false;
    config.compressible = true;
    config.supportsProjectPushdown = true;
    config.extensions = Lists.newArrayList(pluginConfig.getExtension());
    config.fsConf = fsConf;
    config.defaultName = PLUGIN_NAME;
    config.readerOperatorType = CoreOperatorType.REGEX_SUB_SCAN_VALUE;
    return config;
  }

  @Override
  protected ScanBatchCreator scanBatchCreator(OptionManager options) {
    return new LogScanBatchCreator(this);
  }

  @Override
  public RecordReader getRecordReader(FragmentContext context,
                                      DrillFileSystem dfs, FileWork fileWork, List<SchemaPath> columns,
                                      String userName) throws ExecutionSetupException {
    return new LogRecordReader(context, dfs, fileWork,
        columns, userName, getConfig());
  }
}
