package org.apache.drill.exec.store.sumo;

import org.apache.drill.common.logical.StoragePluginConfigBase;
import org.apache.drill.exec.store.base.PlanStringBuilder;
import org.apache.drill.shaded.guava.com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(SumoStoragePluginConfig.NAME)
public class SumoStoragePluginConfig extends StoragePluginConfigBase {

  public static final String NAME = "sumo";

  public final String apiEndpoint;
  public final String accessId;
  public final String accessKey;
  public final String timeZone;
  private final String defaultStartOffset;
  private final String defaultEndOffset;
  public final boolean useReceiptTime;
  public final int shardSizeSecs;

  public SumoStoragePluginConfig(
      @JsonProperty("apiEndpoint") String apiEndpoint,
      @JsonProperty("accessId") String accessId,
      @JsonProperty("accessKey") String accessKey,
      @JsonProperty("timeZone") String timeZone,
      @JsonProperty("defaultStartOffsetSec") String defaultStartOffset,
      @JsonProperty("defaultEndOffsetSec") String defaultEndOffset,
      @JsonProperty("useReceiptTime") boolean useReceiptTime,
      @JsonProperty("shardSizeSecs") int shardSizeSecs ) {
    this.apiEndpoint = apiEndpoint;
    this.accessId = accessId;
    this.accessKey = accessKey;
    this.timeZone = timeZone;
    this.defaultStartOffset = defaultStartOffset;
    this.defaultEndOffset = defaultEndOffset;
    this.useReceiptTime = useReceiptTime;
    this.shardSizeSecs = shardSizeSecs;
  }

  public String getApiEndpoint() { return apiEndpoint; }
  public String getAccessId() { return accessId; }
  public String getAccessKey() { return accessKey; }
  public String getTimeZone() { return timeZone; }
  @JsonProperty("useReceiptTime")
  public boolean useReceiptTime() { return useReceiptTime; }
  public String getDefaultStartOffset() { return defaultStartOffset; }
  public String getDefaultEndOffset() { return defaultEndOffset; }
  public int getShardSizeSecs() { return shardSizeSecs; }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o == null || !(o instanceof SumoStoragePluginConfig)) {
      return false;
    }
    SumoStoragePluginConfig other = (SumoStoragePluginConfig) o;
    return Objects.equal(apiEndpoint, other.apiEndpoint) &&
           Objects.equal(accessId, other.accessId) &&
           Objects.equal(accessKey, other.accessKey) &&
           Objects.equal(timeZone, other.timeZone) &&
           Objects.equal(defaultStartOffset, other.defaultStartOffset) &&
           Objects.equal(defaultEndOffset, other.defaultEndOffset) &&
           useReceiptTime == other.useReceiptTime &&
           shardSizeSecs == other.shardSizeSecs;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(apiEndpoint, accessId, accessKey, timeZone,
        useReceiptTime, defaultStartOffset, defaultEndOffset, shardSizeSecs);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(SumoStoragePluginConfig.NAME)
        .field("apiEndpoint", apiEndpoint)
        .field("accessId", accessId)
        .field("accessKey", accessKey)
        .field("timeZone", timeZone)
        .field("useReceiptTime", useReceiptTime)
        .field("defaultStartOffset", defaultStartOffset)
        .field("defaultEndOffset", defaultEndOffset)
        .field("shardSizeSecs", shardSizeSecs)
        .toString();
  }
}
