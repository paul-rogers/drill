package org.apache.drill.exec.store.sumo;

import java.util.List;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.base.BaseGroupScan;
import org.apache.drill.exec.store.base.PlanStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("sumo-scan")
public class SumoGroupScan extends BaseGroupScan {

  private static final Logger logger = LoggerFactory.getLogger(SumoGroupScan.class);

  private final String schemaName;
  private final String tableName;
  private final SumoQuery sumoQuery;
  private int filterCount;
  private int shardTarget;
  private List<SumoQuery> shards;

  public SumoGroupScan(SumoStoragePlugin storagePlugin, String userName,
      SumoScanSpec scanSpec) {
    super(storagePlugin, userName, null);
    schemaName = scanSpec.schemaName();
    tableName = scanSpec.tableName();
    sumoQuery = scanSpec.sumoQuery();
  }

  public SumoGroupScan(SumoGroupScan from, List<SchemaPath> columns) {
    super(from.storagePlugin, from.getUserName(), columns);
    this.schemaName = from.schemaName;
    this.tableName = from.tableName;
    this.sumoQuery = from.sumoQuery;
  }

  @JsonCreator
  public SumoGroupScan(
      @JsonProperty("config") SumoStoragePluginConfig config,
      @JsonProperty("userName") String userName,
      @JsonProperty("schemaName") String schemaName,
      @JsonProperty("tableName") String tableName,
      @JsonProperty("sumoQuery") SumoQuery sumoQuery,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JacksonInject StoragePluginRegistry engineRegistry) {
    super(config, userName, columns, engineRegistry);
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.sumoQuery = sumoQuery;
    setShardTarget();
  }

  private void setShardTarget() {
    shardTarget = sumoQuery == null ? 1
        : sumoQuery.bestPartitionCount(sumoPlugin().config().getShardSizeSecs());
  }

  public SumoGroupScan(SumoGroupScan from, SumoQuery sumoQuery, int filterCount) {
    super(from);
    this.schemaName = from.schemaName;
    this.tableName = from.tableName;
    this.sumoQuery = sumoQuery;
    this.filterCount = filterCount;
    setShardTarget();
  }

  @JsonProperty("schemaName")
  public String schemaName() { return schemaName; }

  @JsonProperty("tableName")
  public String tableName() { return tableName; }

  @JsonProperty("sumoQuery")
  public SumoQuery sumoQuery() { return sumoQuery; }

  public boolean hasFilters() {
    return filterCount > 0;
  }

  public SumoGroupScan applyFilters(SumoQuery newQuery, int filterCount) {
    return new SumoGroupScan(this, newQuery, filterCount);
  }

  SumoStoragePlugin sumoPlugin() {
    return (SumoStoragePlugin) storagePlugin;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) {
    verifyQuery();
    super.applyAssignments(endpoints);
    SumoQuery resolvedQuery = sumoQuery.rewriteTimes();
    shards = resolvedQuery.shards(sumoPlugin().config().getShardSizeSecs(), endpointCount);
    shardTarget = shards.size();
  }

  private void verifyQuery() {
    String missingCol = sumoQuery.missingColumn();
    if (missingCol != null) {
      throw UserException.validationError()
        .message("The required filter column % is missing for a non-view query", missingCol)
        .build(logger);
    }
  }

  @Override
  @JsonIgnore
  public int getMinParallelizationWidth() {
    return shardTarget;
  }

  @Override
  @JsonIgnore
  public int getMaxParallelizationWidth() {
    return shardTarget;
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) {
    return new SumoSubScan(this, shards.get(minorFragmentId));
  }

  @Override
  public ScanStats computeScanStats() {

    // No good estimates at all, just make up something.

    int estRowCount = 10_000;

    // If filter push down, assume this reduces data size.
    // Need to get Calcite to choose this version rather
    // than the un-filtered version.

    if (filterCount > 0) {
      double filterSelectivity = Math.max(0.001, Math.pow(0.15, filterCount));
      estRowCount = (int) Math.round(estRowCount * filterSelectivity);
    }

    // Assume no disk I/O. So we have to explain costs by reducing CPU.

    double cpuRatio = 1.0;

    // If columns provided, then assume this saves data transfer

    if (getColumns() != BaseGroupScan.ALL_COLUMNS) {
      cpuRatio = 0.75;
    }

    // Would like to reduce network costs, but not easy to do here since Drill
    // scans assume we read from disk, not network.

    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, estRowCount, cpuRatio, 0);
  }

  public boolean isView() {
    return sumoQuery.query() != null;
  }

  @Override
  public void buildPlanString(PlanStringBuilder builder) {
    super.buildPlanString(builder);
    builder.field("schemaName", schemaName);
    builder.field("tableName", tableName);
    builder.field("sumoQuery", sumoQuery);
  }
}
