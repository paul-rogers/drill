package org.apache.drill.exec.store.sumo;

import java.io.IOException;
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
import org.apache.drill.exec.planner.PlannerPhase;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.SessionOptionManager;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.base.BaseScanFactory;
import org.apache.drill.exec.store.base.BaseStoragePlugin;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.fasterxml.jackson.core.type.TypeReference;

public class SumoStoragePlugin extends BaseStoragePlugin<SumoStoragePluginConfig> {

  private static class SumoScanFactory extends
      BaseScanFactory<SumoStoragePlugin, SumoScanSpec, SumoGroupScan, SumoSubScan> {

    @Override
    public SumoGroupScan newGroupScan(SumoStoragePlugin storagePlugin,
        String userName, SumoScanSpec scanSpec,
        SessionOptionManager sessionOptions,
        MetadataProviderManager metadataProviderManager) {

      // User name is not needed for Sumo REST

      return new SumoGroupScan(storagePlugin, "sumo", scanSpec);
    }

    @Override
    public SumoGroupScan groupWithColumns(SumoGroupScan group,
        List<SchemaPath> columns) {
      return new SumoGroupScan(group, columns);
    }

    @Override
    public ScanFrameworkBuilder scanBuilder(SumoStoragePlugin storagePlugin,
        OptionManager options, SumoSubScan subScan) {
      SumoStoragePluginConfig config = storagePlugin.config();
      ScanFrameworkBuilder builder = new ScanFrameworkBuilder();
      storagePlugin.initFramework(builder, subScan);
      builder.setContext(
          new ChildErrorContext(builder.errorContext()) {
            @Override
            public void addContext(UserException.Builder builder) {
              builder.addContext("Endpoint:", config.getApiEndpoint());
            }
          });
      builder.setReaderFactory(new SumoReaderFactory(config, subScan));
      return builder;
    }
  }

  private DateTimeFormatter dateFormat;

  public SumoStoragePlugin(SumoStoragePluginConfig config,
      DrillbitContext context, String name) throws IOException {
    super(context, config, name, buildOptions());
    schemaFactory = new SumoSchemaFactory(this);
  }

  private static StoragePluginOptions buildOptions() {
    StoragePluginOptions options = new StoragePluginOptions();
    options.supportsRead = true;
    options.supportsProjectPushDown = true;
    options.readerId = CoreOperatorType.SUMO_SUB_SCAN_VALUE;
    options.nullType = Types.optional(MinorType.VARCHAR);
    options.scanSpecType = new TypeReference<SumoScanSpec>() { };
    options.scanFactory = new SumoScanFactory();
    return options;
  }

  public DateTimeFormatter timestampFormat() {
    if (dateFormat == null) {
      dateFormat = ISODateTimeFormat.dateTimeNoMillis().withZone(
          DateTimeZone.forID(config().getTimeZone()));
    }
    return dateFormat;
  }

  public long parseDate(String dateStr) {
    return timestampFormat().parseDateTime(dateStr).getMillis();
  }

  @Override
  public Set<? extends StoragePluginOptimizerRule> getOptimizerRules(OptimizerRulesContext optimizerContext, PlannerPhase phase) {

    // Push-down planning is done at the logical phase so it can
    // influence parallelization in the physical phase. Note that many
    // existing plugins perform filter push-down at the physical
    // phase.

    if (phase.isFilterPushDownPhase()) {
      return SumoFilterPushDownListener.rulesFor(optimizerContext, this);
    }
    return ImmutableSet.of();
  }

  private static class SumoReaderFactory implements ReaderFactory {

    private final SumoStoragePluginConfig config;
    private final SumoSubScan scanSpec;
    private int readerCount;

    public SumoReaderFactory(SumoStoragePluginConfig config, SumoSubScan scanSpec) {
      this.scanSpec = scanSpec;
      this.config = config;
    }

    @Override
    public void bind(ManagedScanFramework framework) { }

    @Override
    public ManagedReader<? extends SchemaNegotiator> next() {
      if (readerCount++ > 0) {
        return null;
      }
      return new SumoBatchReader(config, scanSpec.sumoQuery());
    }
  }
}
