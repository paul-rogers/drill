package org.apache.drill.exec.store;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.metastore.MetadataProviderManager;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.store.StoragePlugin.ScanRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ScanRequestImpl implements ScanRequest {
  private static final Logger logger = LoggerFactory.getLogger(ScanRequestImpl.class);

  private final ObjectMapper objectMapper;
  private final String userName;
  private final JSONOptions selection;
  private final List<SchemaPath> columns;
  private final OptionSet options;
  private final MetadataProviderManager providerManager;

  public ScanRequestImpl(StoragePlugin plugin, String userName, JSONOptions selection,
      OptionSet options) {
    this(plugin, userName, selection, null, options, null);
  }

  public ScanRequestImpl(StoragePlugin plugin, String userName, JSONOptions selection,
      OptionSet options, MetadataProviderManager providerManager) {
    this(plugin, userName, selection, null, options, providerManager);
  }

  public ScanRequestImpl(StoragePlugin plugin, String userName, JSONOptions selection,
      List<SchemaPath> columns, OptionSet options, MetadataProviderManager providerManager) {
    this.objectMapper = plugin.pluginContext().objectMapper();
    this.userName = userName;
    this.selection = selection;
    this.columns = columns;
    this.options = options;
    this.providerManager = providerManager;
  }

  @Override
  public String userName() { return userName; }

  @Override
  public JSONOptions jsonOptions() { return selection; }

  @Override
  public <T> T selection(Class<T> selClass) {
    return selection.getWith(objectMapper, selClass);
  }

  @Override
  public <T> T selection(TypeReference<T> ref) {
    try {
      return selection.getListWith(objectMapper, ref);
    } catch (IOException e) {
      throw UserException.validationError(e)
        .addContext("Failed to deserialize the JSON selection for a group scan")
        .addContext("This likely indicates a coding error in getPhysicalScan()")
        .build(logger);
    }
  }

  @Override
  public List<SchemaPath> columns() { return columns; }

  @Override
  public OptionSet options() { return options; }

  @Override
  public MetadataProviderManager metadataProviderManager() {
    return providerManager;
  }
}
