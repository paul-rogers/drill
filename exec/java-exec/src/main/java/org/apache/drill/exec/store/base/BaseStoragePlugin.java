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

import java.io.IOException;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.exceptions.ChildErrorContext;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.metastore.MetadataProviderManager;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework.ScanFrameworkBuilder;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.SessionOptionManager;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class BaseStoragePlugin<C extends StoragePluginConfig>
    extends AbstractStoragePlugin {

  static final Logger logger = LoggerFactory.getLogger(BaseStoragePlugin.class);
  static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();

  public static class StoragePluginOptions {
    public boolean supportsRead;
    public boolean supportsWrite;
    public boolean supportsProjectPushDown;
    public MajorType nullType;
    public int readerId = CoreOperatorType.BASE_SUB_SCAN_VALUE;
    public int writerId;
    public TypeReference<?> scanSpecType;
    public ObjectMapper objectMapper = DEFAULT_MAPPER;
    public BaseScanFactory<?,?,?,?> scanFactory;
  }

  protected final C config;
  protected final StoragePluginOptions options;
  protected SchemaFactory schemaFactory;
  public static final String DEFAULT_SCHEMA_NAME = "default";

  protected BaseStoragePlugin(DrillbitContext context, C config, String name, StoragePluginOptions options) {
    super(context, name);
    this.config = config;
    this.options = options;
    Preconditions.checkNotNull(options.scanSpecType);
    Preconditions.checkNotNull(options.scanFactory);
  }

  @Override
  public StoragePluginConfig getConfig() { return config; }

  public C config() { return config; }

  public StoragePluginOptions options() { return options; }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent)
      throws IOException {
    schemaFactory.registerSchemas(schemaConfig, parent);
  }

  @Override
  public boolean supportsRead() { return options.supportsRead; }

  @Override
  public boolean supportsWrite() { return options.supportsWrite; }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection,
      SessionOptionManager sessionOptions, MetadataProviderManager metadataProviderManager) throws IOException {
    // If this fails, be sure to set the proper class in options.scanSpecClass.
    // Do this in the constructor of your storage plugin
    Object scanSpec = selection.getListWith(options.objectMapper, options.scanSpecType);
    BaseGroupScan groupScan = options.scanFactory.newGroupScanShim(this, userName,
        scanSpec, sessionOptions, metadataProviderManager);
    groupScan.sessionOptions = sessionOptions;
    return groupScan;
  }

  public <T extends BaseSubScan> CloseableRecordBatch createScan(ExecutorFragmentContext context, BaseSubScan subScan)
      throws ExecutionSetupException {
    try {
      final ScanFrameworkBuilder builder =
            options.scanFactory.scanBuilderShim(this, context.getOptions(), subScan);
      return builder.buildScanOperator(context, subScan);
    } catch (final UserException e) {
      // Rethrow user exceptions directly
      throw e;
    } catch (final Throwable e) {
      // Wrap all others
      throw new ExecutionSetupException(e);
    }
  }

  public void initFramework(ScanFrameworkBuilder builder, BaseSubScan subScan) {
    builder.setProjection(subScan.columns());
    builder.setUserName(subScan.getUserName());
    if (options.nullType != null) {
      builder.setNullType(options.nullType);
    }

    // Provide custom error context

    String pluginKind;
    JsonTypeName jsonName = config.getClass().getAnnotation(JsonTypeName.class);
    if (jsonName == null) {
      pluginKind = config.getClass().getSimpleName();
    } else {
      pluginKind = jsonName.value();
    }
    builder.setContext(
        new ChildErrorContext(builder.errorContext()) {
          @Override
          public void addContext(UserException.Builder builder) {
            builder.addContext("Storage plugin config name:", pluginKind);
            builder.addContext("Format plugin class:",
                getClass().getSimpleName());
            builder.addContext("Plugin name:", getName());
          }
        });
  }

  /**
   * Given a storage plugin registry and a storage plugin config, look
   * up the storage plugin. Handles errors by converting them to Drill's
   * usual {@link UserException} form.
   */
  public static BaseStoragePlugin<?> resolvePlugin(StoragePluginRegistry engineRegistry,
      StoragePluginConfig config) {
    try {
      StoragePlugin plugin = engineRegistry.getPlugin(config);
      if (plugin == null) {
        throw UserException.systemError(null)
          .message("Cannot find storage plugin for", config.getClass().getCanonicalName())
          .build(logger);
      }
      if (!(plugin instanceof BaseStoragePlugin)) {
        throw UserException.systemError(null)
          .message("Storage plugin %s is of wrong class: %s but should be %s",
              plugin.getName(), plugin.getClass().getCanonicalName(),
              BaseStoragePlugin.class.getSimpleName())
          .build(logger);
      }
      return (BaseStoragePlugin<?>) plugin;
    } catch (ExecutionSetupException e) {
      throw UserException.systemError(e)
        .message("Cannot find storage plugin for", config.getClass().getCanonicalName())
        .build(logger);
    }
  }
}
