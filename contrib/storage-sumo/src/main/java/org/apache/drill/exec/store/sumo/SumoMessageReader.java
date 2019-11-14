package org.apache.drill.exec.store.sumo;

import java.util.List;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;

import com.sumologic.client.model.LogMessage;
import com.sumologic.client.searchjob.model.GetMessagesForSearchJobResponse;

public class SumoMessageReader extends SumoReader {
  public SumoMessageReader(SumoBatchReader batchReader) {
    super(batchReader);
  }

  @Override
  public boolean next(RowSetLoader writer) {

    GetMessagesForSearchJobResponse response;
    try {
      response = batchReader.sumoClient.getMessagesForSearchJob(batchReader.searchJobId, messageOffset, MESSAGES_PER_REQUEST);
    } catch (Exception e) {
      throw UserException.dataReadError(e)
        .message("Error when reading Sumo search job response")
        .message("Error message", e.getMessage())
        .addContext(batchReader.errorContext())
        .build(logger);
    }
    List<LogMessage> messages = response.getMessages();
    if (messages.isEmpty()) {
      return false;
    }
    defineMessageSchema(writer, response.getFields());
    messageOffset += messages.size();
    for (LogMessage message : messages) {
      writer.start();
      for (FieldShim field : batchSchema) {
        field.write(message.stringField(field.fieldName));
      }
      writer.save();
    }
    return true;
  }
}
