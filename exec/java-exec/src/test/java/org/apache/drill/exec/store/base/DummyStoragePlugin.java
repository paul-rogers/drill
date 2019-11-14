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
package org.apache.drill.exec.store.base;

import java.util.List;
import java.util.Set;

import org.apache.drill.common.exceptions.ChildErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.metastore.MetadataProviderManager;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework.ReaderFactory;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework.ScanFrameworkBuilder;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.SessionOptionManager;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.base.filter.DisjunctionFilterSpec;

import com.fasterxml.jackson.core.type.TypeReference;

public class DummyStoragePlugin
  extends BaseStoragePlugin<DummyStoragePluginConfig> {

  private static class DummyScanFactory extends
        BaseScanFactory<DummyStoragePlugin, DummyScanSpec, DummyGroupScan, DummySubScan> {

    @Override
    public DummyGroupScan newGroupScan(DummyStoragePlugin storagePlugin,
        String userName, DummyScanSpec scanSpec,
        SessionOptionManager sessionOptions,
        MetadataProviderManager metadataProviderManager) {

      // Force user name to "dummy" so golden and actual test files are stable
      return new DummyGroupScan(storagePlugin, "dummy", scanSpec);
    }

    @Override
    public DummyGroupScan groupWithColumns(DummyGroupScan group,
        List<SchemaPath> columns) {
      return new DummyGroupScan(group, columns);
    }

    @Override
    public ScanFrameworkBuilder scanBuilder(DummyStoragePlugin storagePlugin,
        OptionManager options, DummySubScan subScan) {
      ScanFrameworkBuilder builder = new ScanFrameworkBuilder();
      storagePlugin.initFramework(builder, subScan);
      ReaderFactory readerFactory;
      if (subScan.orFilters() != null) {
        readerFactory = new DummyDnfReaderFactory(storagePlugin.config(), subScan);
      } else {
        readerFactory = new DummyReaderFactory(storagePlugin.config(), subScan);
      }
      builder.setReaderFactory(readerFactory);
      builder.setContext(
        new ChildErrorContext(builder.errorContext()) {
          @Override
          public void addContext(UserException.Builder builder) {
            builder.addContext("Table:", subScan.scanSpec().tableName());
          }
        });
      return builder;
    }
  }

  public DummyStoragePlugin(DummyStoragePluginConfig config,
      DrillbitContext context, String name) {
    super(context, config, name, buildOptions(config));
    schemaFactory = new DummySchemaFactory(this);
  }

  private static StoragePluginOptions buildOptions(DummyStoragePluginConfig config) {
    StoragePluginOptions options = new StoragePluginOptions();
    options.supportsRead = true;
    options.supportsProjectPushDown = config.supportProjectPushDown;
    options.maxParallelizationWidth = 1;
    options.nullType = Types.optional(MinorType.VARCHAR);
    options.scanSpecType = new TypeReference<DummyScanSpec>() { };
    options.scanFactory = new DummyScanFactory();
    return options;
  }

  @Override
  public Set<StoragePluginOptimizerRule> getPhysicalOptimizerRules(OptimizerRulesContext optimizerRulesContext) {
    return DummyFilterPushDownListener.rulesFor(optimizerRulesContext, config);
  }

  private static class DummyReaderFactory implements ReaderFactory {

    private final DummyStoragePluginConfig config;
    private final DummySubScan subScan;
    private int readerCount;

    public DummyReaderFactory(DummyStoragePluginConfig config, DummySubScan subScan) {
      this.config = config;
      this.subScan = subScan;
    }

    @Override
    public void bind(ManagedScanFramework framework) { }

    @Override
    public ManagedReader<? extends SchemaNegotiator> next() {
      if (++readerCount > 1) {
        return null;
      }
      return new DummyBatchReader(config, subScan.columns());
    }
  }

  private static class DummyDnfReaderFactory implements ReaderFactory {

    private final DummyStoragePluginConfig config;
    private final DummySubScan subScan;
    private int termIndex;

    public DummyDnfReaderFactory(DummyStoragePluginConfig config, DummySubScan subScan) {
      this.config = config;
      this.subScan = subScan;
    }

    @Override
    public void bind(ManagedScanFramework framework) { }

    @Override
    public ManagedReader<? extends SchemaNegotiator> next() {
      DisjunctionFilterSpec terms = subScan.orFilters();
      if (termIndex >= terms.values.length) {
        return null;
      }
      // We don't actually use the term; just simulate two partitions
      @SuppressWarnings("unused")
      Object partitionValue = terms.values[termIndex++];
      return new DummyBatchReader(config, subScan.columns());
    }
  }
}
