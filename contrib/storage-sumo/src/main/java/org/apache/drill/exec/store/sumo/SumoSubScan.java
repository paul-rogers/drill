package org.apache.drill.exec.store.sumo;

import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.base.BaseSubScan;
import org.apache.drill.exec.store.base.PlanStringBuilder;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("sumo-sub-scan")
public class SumoSubScan extends BaseSubScan {

  private final SumoQuery sumoQuery;

  public SumoSubScan(SumoGroupScan groupScan, SumoQuery sumoQuery) {
    super(groupScan);
    this.sumoQuery = sumoQuery;
  }

  @JsonCreator
  public SumoSubScan(
      @JsonProperty("userName") String userName,
      @JsonProperty("config") SumoStoragePluginConfig config,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("sumoQuery") SumoQuery sumoQuery,
      @JacksonInject StoragePluginRegistry engineRegistry) {
    super(userName, config, columns, engineRegistry);
    this.sumoQuery = sumoQuery;
  }

  @JsonProperty("sumoQuery")
  public SumoQuery sumoQuery() { return sumoQuery; }


  @Override
  public void buildPlanString(PlanStringBuilder builder) {
    super.buildPlanString(builder);
    builder.field("sumoQuery", sumoQuery);
  }
}
