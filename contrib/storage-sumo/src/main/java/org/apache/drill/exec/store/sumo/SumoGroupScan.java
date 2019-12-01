package org.apache.drill.exec.store.sumo;

import java.util.List;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.base.BaseGroupScan;
import org.apache.drill.exec.store.base.PlanStringBuilder;
import org.apache.drill.exec.store.base.filter.DisjunctionFilterSpec;
import org.apache.drill.exec.store.base.filter.RelOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("sumo-scan")
public class SumoGroupScan extends BaseGroupScan {

  private static final Logger logger = LoggerFactory.getLogger(SumoGroupScan.class);

  private final String schemaName;
  private final String tableName;
  private final SumoQuery sumoQuery;
  private final List<RelOp> andFilters;
  private final DisjunctionFilterSpec orFilters;
  private boolean hasFilters;

  public SumoGroupScan(SumoStoragePlugin storagePlugin, String userName,
      SumoScanSpec scanSpec) {
    super(storagePlugin, userName, null);
    schemaName = scanSpec.schemaName();
    tableName = scanSpec.tableName();
    sumoQuery = scanSpec.sumoQuery();
    andFilters = null;
    orFilters = null;
  }

  public SumoGroupScan(SumoGroupScan from, List<SchemaPath> columns) {
    super(from.storagePlugin, from.getUserName(), columns);
    this.schemaName = from.schemaName;
    this.tableName = from.tableName;
    this.sumoQuery = from.sumoQuery;
    this.andFilters = from.andFilters;
    this.orFilters = from.orFilters;
  }

  @JsonCreator
  public SumoGroupScan(
      @JsonProperty("config") SumoStoragePluginConfig config,
      @JsonProperty("userName") String userName,
      @JsonProperty("schemaName") String schemaName,
      @JsonProperty("tableName") String tableName,
      @JsonProperty("sumoQuery") SumoQuery sumoQuery,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("andFilters") List<RelOp> andFilters,
      @JsonProperty("orFilters") DisjunctionFilterSpec orFilters,
      @JacksonInject StoragePluginRegistry engineRegistry) {
    super(config, userName, columns, engineRegistry);
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.sumoQuery = sumoQuery;
    this.andFilters = andFilters;
    this.orFilters = orFilters;
  }

  public SumoGroupScan(SumoGroupScan from,
      List<RelOp> andFilters,
      DisjunctionFilterSpec orFilters) {
    super(from);
    this.schemaName = from.schemaName;
    this.tableName = from.tableName;
    this.sumoQuery = from.sumoQuery;
    this.andFilters = andFilters;
    this.orFilters = orFilters;
  }

  public SumoGroupScan(SumoGroupScan from,
      SumoQuery sumoQuery) {
    super(from);
    this.schemaName = from.schemaName;
    this.tableName = from.tableName;
    this.sumoQuery = sumoQuery;
    this.andFilters = from.andFilters;
    this.orFilters = from.orFilters;
  }

  @JsonProperty("schemaName")
  public String schemaName() { return schemaName; }

  @JsonProperty("tableName")
  public String tableName() { return tableName; }

  @JsonProperty("sumoQuery")
  public SumoQuery sumoQuery() { return sumoQuery; }

  @JsonProperty("andFilters")
  public List<RelOp> andFilters() { return andFilters; }

  @JsonProperty("orFilters")
  public DisjunctionFilterSpec orFilters() { return orFilters; }

  public boolean hasFilters() {
    return hasFilters;
  }

  public SumoGroupScan applyFilters(SumoQuery newQuery) {
    SumoGroupScan newScan = new SumoGroupScan(this, newQuery);
    newScan.hasFilters = true;
    return newScan;
  }

  SumoStoragePlugin sumoPlugin() {
    return (SumoStoragePlugin) storagePlugin;
  }

  @Override
  public int getMaxParallelizationWidth() {

  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) {
    String missingCol = sumoQuery.missingColumn();
    if (missingCol != null) {
      throw UserException.validationError()
        .message("The required filter column % is missing for a non-view query", missingCol)
        .build(logger);
    }

    SumoQuery resolvedQuery = sumoQuery.rewriteTimes();
    // TODO: Split
    return new SumoSubScan(this, resolvedQuery);
  }

  @Override
  public ScanStats computeScanStats() {

    // No good estimates at all, just make up something.

    int estRowCount = 10_000;

    // If filter push down, assume this reduces data size.
    // Just need to get Calcite to choose this version rather
    // than the un-filtered version.

    if (hasFilters()) {
      estRowCount /= 2;
    }

    // Make up an average row width.

    int estDataSize = estRowCount * 200;

    // If columns provided, then assume this saves data transfer

    if (getColumns() != BaseGroupScan.ALL_COLUMNS) {
      estDataSize = estDataSize * 3 / 4;
    }
    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, estRowCount, 1, estDataSize);
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
    builder.field("andFilters", andFilters);
    builder.field("orFilters", orFilters);
  }
}
