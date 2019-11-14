package org.apache.drill.exec.store.sumo;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.exec.store.base.PlanStringBuilder;
import org.apache.drill.exec.store.base.filter.FilterSpec;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.collect.Lists;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a single Sumo REST query. Data is in the form
 * the Sumo API expects (dates as ISO-encoded strings.)
 */

@JsonInclude(Include.NON_NULL)
public class SumoQuery {

  public static final String QUERY_COL = "query";
  public static final String END_TIME_COL = "endtime";
  public static final String START_TIME_COL = "starttime";

  private final String query;
  private final boolean isAggregate;
  private final String startTime;
  private final String endTime;
  private final String timeZone;
  private final boolean useReceiptTime;

  @JsonCreator
  public SumoQuery(
      @JsonProperty("query") String query,
      @JsonProperty("isAggregate") boolean isAggregate,
      @JsonProperty("startTime") String startTime,
      @JsonProperty("endTime") String endTime,
      @JsonProperty("timeZone") String timeZone,
      @JsonProperty("useReceiptTime") boolean useReceiptTime) {
    this.query = query;
    this.isAggregate = isAggregate;
    this.startTime = startTime;
    this.endTime = endTime;
    this.timeZone = timeZone;
    this.useReceiptTime = useReceiptTime;
  }

  public SumoQuery(String timeZone, boolean useReceiptTime) {
    this(null, false, null, null, timeZone, useReceiptTime);
  }

  @JsonProperty("query")
  public String query() { return query; }

  @JsonProperty("isAggregate")
  public boolean isAggregate() { return isAggregate; }

  @JsonProperty("startTime")
  public String startTime() { return startTime; }

  @JsonProperty("endTime")
  public String endTime() { return endTime; }

  @JsonProperty("timeZone")
  public String timeZone() { return timeZone; }

  @JsonProperty("useReceiptTime")
  public boolean useReceiptTime() { return useReceiptTime; }

  @JsonIgnore
  public boolean isValid() {
    return missingColumn() == null;
  }

  public String missingColumn() {
    if (query == null) {
      return QUERY_COL;
    } else if (startTime == null) {
      return START_TIME_COL;
    } else if (endTime == null) {
      return END_TIME_COL;
    } else {
      return null;
    }
  }

  public SumoQuery rewrite(String query, String startTime, String endTime) {
    if (query == null && startTime == null && endTime == null) {
      return this;
    }
    return new SumoQuery(
        query == null ? this.query : query,
        this.isAggregate,
        startTime == null ? this.startTime : startTime,
        endTime == null ? this.endTime : endTime,
        timeZone, useReceiptTime);
  }

  public SumoQuery rewrite(FilterSpec filterSpec) {
    if (filterSpec == null) {
      return this;
    }
    return new SumoQuery(
        query, isAggregate, startTime, endTime,
        timeZone, useReceiptTime);
  }

  public int bestPartitionCount(int shardSizeSec) {
    if (isAggregate() || shardSizeSec == 0 || missingColumn() != null) {
      return 1;
    }
    DateTimeZone tz = DateTimeZone.forID(timeZone);
    DateTimeFormatter dateFormat = ISODateTimeFormat.dateHourMinuteSecond().withZone(tz);
    long startTimeStamp = dateFormat.parseMillis(startTime);
    long endTimeStamp = dateFormat.parseMillis(endTime);
    long duration = endTimeStamp - startTimeStamp;
    return Math.max(1, (int) (duration / shardSizeSec));
  }

  public List<String> partition(int shardSizeSec, int maxSliceCount) {
    DateTimeZone tz = DateTimeZone.forID(timeZone);
    DateTimeFormatter dateFormat = ISODateTimeFormat.dateHourMinuteSecond().withZone(tz);
    long startTimeStamp = dateFormat.parseMillis(startTime);
    long endTimeStamp = dateFormat.parseMillis(endTime);
    long duration = endTimeStamp - startTimeStamp;
    int shardCount = Math.min(maxSliceCount, Math.max(1, (int) (duration / shardSizeSec)));
    long shardDuration = duration / shardCount;

    List<String> bounds = new ArrayList<>();
    bounds.add(startTime);
    for (int i = 1; i < shardCount; i++) {
      bounds.add(dateFormat.print(startTimeStamp + shardDuration * shardDuration * i));
    }
    bounds.add(endTime);
    return bounds;
  }

  public SumoQuery shardAt(List<String> timeBounds, int shardNo) {
    String shardStartTime = timeBounds.get(shardNo);
    String shardEndTime = timeBounds.get(shardNo + 1);
    return new SumoQuery(query, isAggregate, shardStartTime, shardEndTime,
        timeZone, useReceiptTime);
  }

  public List<SumoQuery> shards(int shardSizeSec, int maxSliceCount) {
    if (isAggregate() || maxSliceCount == 1 || shardSizeSec == 0) {
      return Lists.newArrayList(this);
    }
    List<String> bounds = partition(shardSizeSec, maxSliceCount);
    List<SumoQuery> slices = new ArrayList<>();
    for (int i = 0; i < bounds.size() - 1; i++) {
      slices.add(shardAt(bounds, i));
    }
    return slices;
  }

  public SumoQuery rewriteTimes() {
    Preconditions.checkState(isValid());
    Long startOffset = parseRelative(startTime);
    Long endOffset = parseRelative(endTime);

    // If neither time is relative, just use them as-is.

    if (startOffset == null && endOffset == null) {
      return this;
    }

    DateTimeZone tz = DateTimeZone.forID(timeZone);
    DateTimeFormatter dateFormat = ISODateTimeFormat.dateHourMinuteSecond().withZone(tz);

    // Start time: as given, or relative to current time

    long startTs;
    String newStartTime;
    if (startOffset == null) {
      newStartTime = startTime;
      startTs = dateFormat.parseMillis(startTime);
    } else {
      startTs = tz.convertUTCToLocal(System.currentTimeMillis());
      newStartTime = dateFormat.print(startTs);
    }

    // End time: as given, or relative to start time

    String newEndTime;
    long endTs;
    if (endOffset == null) {
      newEndTime = endTime;
      endTs = dateFormat.parseMillis(startTime);
    } else {
      endTs = startTs + endOffset;
      newEndTime = dateFormat.print(endTs);
    }

    // If offsets caused end before start, reverse them.

    if (endTs < startTs) {
      String temp = newStartTime;
      newStartTime = newEndTime;
      newStartTime = temp;
    }
    return new SumoQuery(query, isAggregate, newStartTime, newEndTime,
        timeZone, useReceiptTime);
  }

  private static final Pattern RELATIVE_TIME_PATTERN = Pattern.compile("(-?\\d+)(s|m|h)");

  public static Long parseRelative(String timeStr) {
    Matcher m = RELATIVE_TIME_PATTERN.matcher(timeStr.trim().toLowerCase());
    if (!m.matches()) {
      return null;
    }
    int amount = Integer.parseInt(m.group(1));
    switch (m.group(2)) {
    case "s":
      break;
    case "m":
      amount *= 60;
      break;
    case "h":
      amount *= 60 * 60;
      break;
    default:
      throw new IllegalArgumentException(
          String.format("Not a valid time unit: \"%s\"", timeStr));
    }
    return amount * 1000L;
  }

  public SumoQuery applyDefaults(String defaultStartTime, String defaultEndTime, String defaultTimeZone) {
    if (isValid()) {
      return this;
    }
    return new SumoQuery(query, isAggregate,
        startTime == null ? defaultStartTime : startTime,
        endTime == null ? defaultEndTime : endTime,
        timeZone == null ? defaultTimeZone : timeZone,
        useReceiptTime);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
        .field("query", query)
        .field("startTime", startTime)
        .field("endTime", endTime)
        .field("timeZone", timeZone)
        .field("useReceiptTime", useReceiptTime)
        .toString();
  }
}
