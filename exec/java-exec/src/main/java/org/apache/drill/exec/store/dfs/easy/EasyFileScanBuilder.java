package org.apache.drill.exec.store.dfs.easy;

import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException.Builder;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileScanLifecycleBuilder;

/**
 * Create the file scan lifecycle that manages the scan. The lifecycle
 * creates batch readers one by one for each file or block. It defines semantic
 * rules for projection. It handles a provided schema. Also handles "early"
 * or "late" schema readers.
 * <p>
 * This is for "version 2" of EVF. Newer code should use this version
 */
public class EasyFileScanBuilder extends FileScanLifecycleBuilder {

  public static class EvfErrorContext implements CustomErrorContext {
    private final EasySubScan scan;
    private final EasyFormatPlugin<? extends FormatPluginConfig> plugin;

    public EvfErrorContext(EasySubScan scan,
        EasyFormatPlugin<? extends FormatPluginConfig> plugin) {
      this.scan = scan;
      this.plugin = plugin;
    }

    @Override
    public void addContext(Builder builder) {
      builder
        .addContext("Format plugin type", plugin.easyConfig().defaultName)
        .addContext("Format plugin class", plugin.getClass().getSimpleName())
        .addContext("Plugin config name", plugin.getName());
      if (scan.getSelectionRoot() != null) {
        builder.addContext("Table directory", scan.getSelectionRoot().toString());
      }
    }
  }

  /**
   * Constructor
   *
   * @param scan the physical operation definition for the scan operation. Contains
   * one or more files to read. (The Easy format plugin works only for files.)
   * @return the scan framework which orchestrates the scan operation across
   * potentially many files
   * @throws ExecutionSetupException for all setup failures
   */
  public EasyFileScanBuilder(FragmentContext context, EasySubScan scan,
      EasyFormatPlugin<? extends FormatPluginConfig> plugin) {

    options(context.getOptions());
    projection(scan.getColumns());
    userName(scan.getUserName());
    providedSchema(scan.getSchema());
    fileSystemConfig(plugin.easyConfig().fsConf);
    fileSplitImpls(scan.getWorkUnits());
    rootDir(scan.getSelectionRoot());
    maxPartitionDepth(scan.getPartitionDepth());
    errorContext(new EvfErrorContext(scan, plugin));
  }
}
