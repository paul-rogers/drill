package org.apache.drill.exec.store.sumo;

import java.net.MalformedURLException;

import org.apache.drill.common.exceptions.ChildErrorContext;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.project.RequestedTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sumologic.client.Credentials;
import com.sumologic.client.SumoLogicClient;
import com.sumologic.client.searchjob.model.GetSearchJobStatusResponse;

/**
 * Drill reader for the Sumo search job API.
 * <p>
 * This version uses the client which Sumo provides. A better design would be to
 * read the JSON with Drill's JSON parser to avoid the need to materialize the
 * JSON structure, but this is left as an exercise for later.
 *
 * @see <a href="https://help.sumologic.com/APIs/Search-Job-API">Sumo documentation</a>
 * @see <a href="https://github.com/SumoLogic/sumo-java-client">Sumo Java client</a>
 * @see <a href="https://github.com/SumoLogic/sumo-java-client/blob/master/src/main/java/com/sumologic/client/searchjob/SearchJobExample.java">
 * Example client</a>
 */

public class SumoBatchReader implements ManagedReader<SchemaNegotiator> {

  private static final Logger logger = LoggerFactory.getLogger(SumoBatchReader.class);

  public static final String GATHERING_RESULTS_STATUS = "GATHERING RESULTS";
  public static final String RESULTS_READY_STATUS = "DONE GATHERING RESULTS";
  public static final String CANCELLED_STATUS = "CANCELLED";

  private final SumoStoragePluginConfig pluginConfig;
  private final SumoQuery query;
  private CustomErrorContext parentErrorContext;
  protected SumoLogicClient sumoClient;
  protected String searchJobId;
  private int expectedMessageCount;
  private int expectedMecordCount;
  private SumoReader sumoReader;
  private ResultSetLoader resultLoader;
  protected RequestedTuple rootProjection;

  public SumoBatchReader(SumoStoragePluginConfig pluginConfig, SumoQuery query) {
    this.pluginConfig = pluginConfig;
    this.query = query;
  }

  @Override
  public boolean open(SchemaNegotiator negotiator) {
    parentErrorContext= negotiator.parentErrorContext();
    Credentials credential = new Credentials(
        pluginConfig.getAccessId(), pluginConfig.getAccessKey());
    sumoClient = new SumoLogicClient(credential);
    try {
      sumoClient.setURL(pluginConfig.getApiEndpoint());
    } catch (MalformedURLException e) {
      throw UserException.dataReadError(e)
        .message("Invalid SUMO endpoint URL")
        .message("Error message", e.getMessage())
        // Error is independent of this query
        .addContext(parentErrorContext)
        .build(logger);
    }
    try {
      searchJobId = sumoClient.createSearchJob(
          query.query(), query.startTime(), query.endTime(),
          query.timeZone(), Boolean.toString(query.useReceiptTime()));
    } catch (Exception e) {
      throw UserException.dataReadError(e)
        .message("Failed to create Sumo search job")
        .addContext(errorContext())
        .build(logger);
    }
    long startTime = System.currentTimeMillis();
    waitForData();
    System.out.println(String.format("Prep time ms.: %d", System.currentTimeMillis() - startTime));
    resultLoader = negotiator.build();
    rootProjection = negotiator.rootProjection();
    return true;
  }

  protected CustomErrorContext errorContext() {
    return new ChildErrorContext(parentErrorContext) {
      @Override
      public void addContext(UserException.Builder builder) {
        super.addContext(builder);
        builder.addContext("Query:", query.query());
        builder.addContext("Start time:", query.startTime());
        builder.addContext("End time:", query.endTime());
        builder.addContext("Time zone:", query.timeZone());
        builder.addContext("Use Receipt Time:",
            Boolean.toString(query.useReceiptTime()));
      }
    };
  }

  private void waitForData() {
    int delayCount = 0;
    int totalWaitSec = 0;
    for (;;) {
      GetSearchJobStatusResponse status;
      try {
        status = sumoClient.getSearchJobStatus(searchJobId);
      } catch (Exception e) {
        throw UserException.dataReadError(e)
          .message("Failed to create Sumo search job")
          .message("Error message", e.getMessage())
          .addContext(errorContext())
          .build(logger);
      }
      if (status == null) {
        throw UserException.dataReadError()
          .message("Sumo search job status was null")
          .addContext(errorContext())
          .build(logger);
      }
      switch (status.getState()) {
      case RESULTS_READY_STATUS:
        expectedMessageCount = status.getMessageCount();
        expectedMecordCount = status.getRecordCount();
        logger.info( "Query: {}", query.query());
        logger.info("Start Time: {}, End Time: {}, Time Zone: {}",
            query.startTime(), query.endTime(), query.timeZone());
        logger.info("Returned {} messages, {} records after waiting {} seconds",
            expectedMessageCount, expectedMecordCount, totalWaitSec);
        if (expectedMecordCount > 0 && expectedMessageCount != 0) {
          sumoReader = new SumoRecordReader(this);
        } else {
          sumoReader = new SumoMessageReader(this);
        }
        return;
      case CANCELLED_STATUS:
        throw UserException.dataReadError()
          .message("Sumo search job was cancelled unexpectedly")
          .addContext(errorContext())
          .build(logger);
      case GATHERING_RESULTS_STATUS:

        // Not ready, give a bit more time. Adaptive; simple
        // queries wait only a little, we wait longer as the query
        // takes longer.

        int waitSec = Math.max(++delayCount, 5);
        totalWaitSec += waitSec;
        try {
          Thread.sleep(1000 * waitSec);
        } catch (InterruptedException e) {
          // Should never occur
        }
        break;
      default:
        throw UserException.dataReadError()
          .message("Unexpected Sumo searcj job status response")
          .addContext("Status", status.getState())
          .addContext(errorContext())
          .build(logger);
      }
    }
  }

  /**
   * Read the next output batch of Sumo results. Sumo delivers results in
   * its own batches (the size of which we define.) Our goal is to pack
   * the (smaller) Sumo batches into the (larger) Drill batches. But, we
   * don't know how many Sumo batches will fit; maybe less than 1, maybe
   * an even number, maybe 2.5; we just don't know. So, we have to track
   * input and output batches indecently, then match them up.
   */
  @Override
  public boolean next() {
    return sumoReader.next(resultLoader.writer());
  }

  @Override
  public void close() {
    if (sumoClient != null) {
      sumoClient.cancelSearchJob(searchJobId);
      sumoClient = null;
      sumoReader = null;
    }
  }
}
