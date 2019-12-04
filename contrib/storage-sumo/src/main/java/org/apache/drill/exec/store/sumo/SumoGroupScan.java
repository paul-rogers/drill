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

/**
 * Represents a query on top of the Sumo Search Job API, including:
 * <ul>
 * <li>Named views, currently implemented via a hard-coded set of
 * names since Sumo does not support named queries, only materialized
 * ("scheduled") views.</li>
 * <li>Filter-push-down for query text (<code>`query` = 'query text'</code>),
 * and for start and end times (<code>startTime = 'ISO time'</code>.)</li>
 * <li>Parallelizing detail queries. (This is a bit of a hack since
 * there is no a-prori way to know a query is detail or aggregate.</li>
 * <li>Relative times, such as <code>2m</code> to indicate end time
 * should be two minutes after the start time.</li>
 * </ul>
 * <p>
 * A complexity is that the Drill planning process is pretty loose
 * and ad-hoc: there is no fixed call sequence, instead there are
 * events that arrive or do not arrive. This fact makes the above
 * features very tricky to implement. We need to enforce an ordering:
 * <ul>
 * <li>Get the query "template" from a view, including query text,
 * start time and end time. Times can be relative.</li>
 * <li>Apply filter-push-down fields which can be any combination
 * of query, start time and end time.</li>
 * <li>At this point, the query
 * should be "complete" with all fields filled in. If filter push-down
 * does <i>not</i> occur, then the view itself must be complete.
 * We can therefore rewrite the query to replace relative times with
 * absolute times.</li>
 * <li>If the query is detail, compute the time duration divide by
 * the configured shared size, to get the desired parallelism.</li>
 * <li>Drill provides the actual available number of endpoints (degree
 * of parallelism.) Recompute the shard size to fit.</li>
 * </ul>
 * Points of complexity:
 * <ul>
 * <li>Drill will ask for the shard count <i>before</i> filter push-down
 * which provides the information to compute the shards. These calls
 * require a dummy number.</li>
 * <li>Drill does not tell us when there is no filter push-down, thus
 * we don't have a definite point at which we know that, if there were
 * filters, they would have been applied.</li>
 * <li>Drill will again ask for shard count, but now we should be able
 * to translate relative dates to absolute and provide a count.</li>
 * </ul>
 * Given the above constraints, the final design is:
 * <ul>
 * <li>Maintain a flag, <code>finalizedSumoQuery</code> that tell us
 * if we've finalized the query (checked required fields, converted
 * relative times to absolute.</li>
 * <li>Observe that the calls to get min/max parallelization width
 * happen only in the physical planning stage. Check the flag and
 * finalize the query on the first call.</li>
 * <li>When finalizing, create an estimated shard count which is the
 * maximum number of possible shards.</li>
 * <li>Later, when Drill offers the set of endpoints in
 * <code>applyAssignments()</code>, compute the actual shard count
 * which may be lower than the ideal computed above. Then, create
 * the shards as new, narrowed Sumo queries.</li>
 * </ul>
 */
@JsonTypeName("sumo-scan")
public class SumoGroupScan extends BaseGroupScan {

  private static final Logger logger = LoggerFactory.getLogger(SumoGroupScan.class);

  private final String schemaName;
  private final String tableName;
  private SumoQuery sumoQuery;

  /**
   * Count of pushed-down filters. Required to adjust cost estimate
   * to reflect filter selectivity, which is required so that Calcite
   * will choose the push-down plan rather than the original.
   */
  private int filterCount;

  /**
   * There is no good event from Drill to tell us that logical
   * planning is complete and we're on to physical planning. We want
   * to know that because we will finalize the query (check for missing
   * fields, convert relative times to absolute) once logical planning
   * is done. Instead, we just track if we have finalized the query on
   * any physical-related call, and do the finalization if needed.
   */
  private boolean finalizedSumoQuery;

  /**
   * The number of shards for this query. Drives parallelization.
   */
  private int shardTarget;

  /**
   * Sumo query rewritten into shards.
   */
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
  }

  public SumoGroupScan(SumoGroupScan from, SumoQuery sumoQuery, int filterCount) {
    super(from);
    this.schemaName = from.schemaName;
    this.tableName = from.tableName;
    this.sumoQuery = sumoQuery;
    this.filterCount = filterCount;
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
    finalizeQuery();
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
    finalizeQuery();
    return shardTarget;
  }

  @Override
  @JsonIgnore
  public int getMaxParallelizationWidth() {
    finalizeQuery();
    return shardTarget;
  }

  private void finalizeQuery() {
    if (finalizedSumoQuery) {
      return;
    }
    verifyQuery();
    sumoQuery = sumoQuery.rewriteTimes();
    shardTarget = sumoQuery == null ? 1
        : sumoQuery.bestPartitionCount(sumoPlugin().config().getShardSizeSecs());
    finalizedSumoQuery = true;
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
