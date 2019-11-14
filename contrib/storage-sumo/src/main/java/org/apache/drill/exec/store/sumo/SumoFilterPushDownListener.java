package org.apache.drill.exec.store.sumo;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.base.filter.ConstantHolder;
import org.apache.drill.exec.store.base.filter.DisjunctionFilterSpec;
import org.apache.drill.exec.store.base.filter.FilterPushDownListener;
import org.apache.drill.exec.store.base.filter.FilterPushDownStrategy;
import org.apache.drill.exec.store.base.filter.RelOp;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class SumoFilterPushDownListener implements FilterPushDownListener {

  private static final Logger logger = LoggerFactory.getLogger(SumoFilterPushDownListener.class);

  public static abstract class SumoFilterCol {

    public RelOp accept(RelOp relop, DateTimeFormatter timestampFormat) {
      if (relop.op != RelOp.Op.EQ) {
        return null;
      }
      ConstantHolder normalized = convert(relop.value, timestampFormat);
      if (normalized == null) {
        return null;
      }
      return relop.rewrite(relop.colName.toLowerCase(), normalized);
    }

    protected abstract ConstantHolder convert(ConstantHolder value, DateTimeFormatter timestampFormat);

    public boolean isPartitionCol() { return false; }
  }

  public static class SumoTimeFilter extends SumoFilterCol {

    @Override
    protected ConstantHolder convert(ConstantHolder value, DateTimeFormatter timestampFormat) {
      String timestamp;
      switch(value.type) {
      case VARCHAR:
        return value;
      case TIMESTAMP: {
        // Drill timestamps are in the local time of the server
        long ts = (Long) value.value;
        ts -= TimeZone.getDefault().getOffset(ts);
        ts += timestampFormat.getZone().getOffset(ts);
        timestamp = timestampFormat.print(ts);
        break;
      }
      case BIGINT:
        timestamp = timestampFormat.print((Long) value.value);
        break;
      default:
        return null;
      }
      return new ConstantHolder(MinorType.VARCHAR, timestamp);
    }
  }

 public static class SumoQueryFilter extends SumoFilterCol {

    @Override
    protected ConstantHolder convert(ConstantHolder value, DateTimeFormatter timestampFormat) {
      return value.type == MinorType.VARCHAR ? value : null;
    }
  }

  public static final Map<String, SumoFilterCol> SUMO_PUSH_DOWN_COLS;

  static {
    SUMO_PUSH_DOWN_COLS = new HashMap<>();
    SUMO_PUSH_DOWN_COLS.put(SumoQuery.START_TIME_COL, new SumoTimeFilter());
    SUMO_PUSH_DOWN_COLS.put(SumoQuery.END_TIME_COL, new SumoTimeFilter());
    SUMO_PUSH_DOWN_COLS.put(SumoQuery.QUERY_COL, new SumoQueryFilter());
  }

  private final DateTimeFormatter timestampFormat;

  public SumoFilterPushDownListener(DateTimeFormatter timestampFormat) {
    this.timestampFormat = timestampFormat;
  }

  public static Set<StoragePluginOptimizerRule> rulesFor(
      OptimizerRulesContext optimizerRulesContext,
      SumoStoragePlugin plugin) {
    return FilterPushDownStrategy.rulesFor(optimizerRulesContext,
        new SumoFilterPushDownListener(plugin.timestampFormat()));
  }

  @Override
  public String prefix() { return "Sumo"; }

  @Override
  public boolean isTargetScan(ScanPrel scan) {
     return scan.getGroupScan() instanceof SumoGroupScan;
  }

  @Override
  public boolean needsApplication(ScanPrel scan) {
    SumoGroupScan groupScan = (SumoGroupScan) scan.getGroupScan();
    return !groupScan.hasFilters();
  }

  @Override
  public RelOp accept(ScanPrel scan, RelOp relOp) {
    SumoFilterCol sumoCol = SUMO_PUSH_DOWN_COLS.get(relOp.colName.toLowerCase());
    if (sumoCol == null) {
      return null;
    }
    return sumoCol.accept(relOp, timestampFormat);
  }

  @Override
  public Pair<GroupScan, List<RexNode>> transform(ScanPrel scan,
      List<Pair<RexNode, RelOp>> andTerms, Pair<RexNode, DisjunctionFilterSpec> orTerm) {

    SumoGroupScan groupScan = (SumoGroupScan) scan.getGroupScan();
    SumoQuery newQuery = applyAndTerms(groupScan, andTerms);
    Pair<RexNode, SumoQuery> orResult = applyOrTerms(groupScan, newQuery, orTerm);
    newQuery = orResult.right;

    if (newQuery == groupScan.sumoQuery()) {
      // Something went wrong; all filters were rejected
      return null;
    }
    GroupScan newScan = groupScan.applyFilters(newQuery);
    List<RexNode> rejected = orResult.left == null ? null : Collections.singletonList(orResult.left);
    return Pair.of(newScan, rejected);
  }

  private SumoQuery applyAndTerms(SumoGroupScan groupScan, List<Pair<RexNode, RelOp>> andTerms) {
    List<RelOp> relOps = andTerms.stream().map(t -> t.right).collect(Collectors.toList());
    SumoQuery sumoQuery = groupScan.sumoQuery();
    String startTime = null;
    String endTime = null;
    String queryStr = null;
    for (RelOp relop : relOps) {
      switch (relop.colName) {
      case SumoQuery.START_TIME_COL:
        startTime = (String) relop.value.value;
        break;
      case SumoQuery.END_TIME_COL:
        endTime = (String) relop.value.value;
        break;
      case SumoQuery.QUERY_COL:
        if (sumoQuery.query() != null) {
          throw UserException.validationError()
            .message("View %s already has a query, cannnot specifiy another in the SQL query",
                groupScan.tableName())
            .build(logger);
        }
        queryStr = (String) relop.value.value;
        break;
      default:
        // Should never get here
        Preconditions.checkState(false);
      }
    }
    return sumoQuery.rewrite(queryStr, startTime, endTime);
  }

  private Pair<RexNode, SumoQuery> applyOrTerms(SumoGroupScan groupScan, SumoQuery sumoQuery,
      Pair<RexNode, DisjunctionFilterSpec> orTerm) {

    if (orTerm == null) {
      return Pair.of(null, sumoQuery);
    } else if (!acceptPartitionColumn(orTerm)) {
      return Pair.of(orTerm.left, sumoQuery);
    } else {
      return Pair.of(null, sumoQuery.rewrite(orTerm.right));
    }
  }

  private boolean acceptPartitionColumn(Pair<RexNode, DisjunctionFilterSpec> orTerm) {
    DisjunctionFilterSpec orSpec = orTerm.right;
    SumoFilterCol sumoCol = SUMO_PUSH_DOWN_COLS.get(orSpec.column.toLowerCase());

    // Sanity check: should not have gotten here if column is undefined
    Preconditions.checkNotNull(sumoCol);

    // If can't partition by this column, reject it.

    return sumoCol.isPartitionCol();
  }
}
