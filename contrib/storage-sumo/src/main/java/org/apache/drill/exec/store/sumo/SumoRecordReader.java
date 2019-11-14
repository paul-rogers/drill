package org.apache.drill.exec.store.sumo;

import java.util.List;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;

import com.sumologic.client.searchjob.model.GetRecordsForSearchJobResponse;
import com.sumologic.client.searchjob.model.SearchJobRecord;

/**
 * The Sumo record API returns a map of strings, with a separate
 * map per record, with no metadata information available. Unfortunately,
 * it falls to the Drill query to convert string values to numerics
 * since there is no a-priori way to know the meaning of record
 * columns.
 */

public class SumoRecordReader extends SumoReader {

  public SumoRecordReader(SumoBatchReader batchReader) {
    super(batchReader);
  }

  @Override
  public boolean next(RowSetLoader writer) {
    GetRecordsForSearchJobResponse response;
    try {
      response = batchReader.sumoClient.getRecordsForSearchJob(batchReader.searchJobId, messageOffset, MESSAGES_PER_REQUEST);
    } catch (Exception e) {
      throw UserException.dataReadError(e)
        .message("Error when reading Sumo search job response")
        .message("Error message", e.getMessage())
        .addContext(batchReader.errorContext())
        .build(logger);
    }
    List<SearchJobRecord> records = response.getRecords();
    if (records.isEmpty()) {
      return false;
    }
    defineMessageSchema(writer, response.getFields());
    messageOffset += records.size();
    for (SearchJobRecord record : records) {
      writer.start();
      for (FieldShim field : batchSchema) {
        field.write(record.stringField(field.fieldName));
      }
      writer.save();
    }
    return true;
  }
}
