package org.apache.drill.exec.store.sumo;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.exec.store.base.PlanStringBuilder;
import org.apache.drill.exec.store.base.filter.DisjunctionFilterSpec;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

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
  private final String startTime;
  private final String endTime;
  private final String timeZone;
  private final boolean useReceiptTime;
  private final DisjunctionFilterSpec partitionSpec;

  @JsonCreator
  public SumoQuery(
      @JsonProperty("query") String query,
      @JsonProperty("startTime") String startTime,
      @JsonProperty("endTime") String endTime,
      @JsonProperty("timeZone") String timeZone,
      @JsonProperty("useReceiptTime") boolean useReceiptTime,
      @JsonProperty("partitionSpec") DisjunctionFilterSpec partitionSpec) {
    this.query = query;
    this.startTime = startTime;
    this.endTime = endTime;
    this.timeZone = timeZone;
    this.useReceiptTime = useReceiptTime;
    this.partitionSpec = partitionSpec;
  }

  public SumoQuery(String timeZone, boolean useReceiptTime) {
    this(null, null, null, timeZone, useReceiptTime, null);
  }

  @JsonProperty("query")
  public String query() { return query; }

  @JsonProperty("startTime")
  public String startTime() { return startTime; }

  @JsonProperty("endTime")
  public String endTime() { return endTime; }

  @JsonProperty("timeZone")
  public String timeZone() { return timeZone; }

  @JsonProperty("useReceiptTime")
  public boolean useReceiptTime() { return useReceiptTime; }

  @JsonProperty("partitionSpec")
  public DisjunctionFilterSpec partitionSpec() { return partitionSpec; }

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
        startTime == null ? this.startTime : startTime,
        endTime == null ? this.endTime : endTime,
        timeZone, useReceiptTime, partitionSpec);
  }

  public SumoQuery rewrite(DisjunctionFilterSpec partitionSpec) {
    if (partitionSpec == null) {
      return this;
    }
    return new SumoQuery(
        query, startTime, endTime,
        timeZone, useReceiptTime, partitionSpec);
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
    return new SumoQuery(query, newStartTime, newEndTime,
        timeZone, useReceiptTime, partitionSpec);
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
    return new SumoQuery(query,
        startTime == null ? defaultStartTime : startTime,
        endTime == null ? defaultEndTime : endTime,
        timeZone == null ? defaultTimeZone : timeZone,
        useReceiptTime, partitionSpec);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
        .field("query", query)
        .field("startTime", startTime)
        .field("endTime", endTime)
        .field("timeZone", timeZone)
        .field("useReceiptTime", useReceiptTime)
        .field("partitionSpec", partitionSpec)
        .toString();
  }
}
