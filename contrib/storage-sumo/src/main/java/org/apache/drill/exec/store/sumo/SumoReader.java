package org.apache.drill.exec.store.sumo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.directory.api.util.Strings;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.physical.resultSet.project.RequestedTuple;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sumologic.client.searchjob.model.SearchJobField;

/**
 * Turns out that the each query can return both messages and records.
 * These are just different enough to be annoying: the class for messages
 * and records are identical, but are copies of each other making it
 * hard to use the type-conversion methods in those classes.
 */

public abstract class SumoReader {

  protected static final Logger logger = LoggerFactory.getLogger(SumoReader.class);

  public static final int MESSAGES_PER_REQUEST = 100;
  public static final String SUMO_STRING_TYPE = "string";
  public static final String SUMO_LONG_TYPE = "long";
  public static final String SUMO_INT_TYPE = "int";

  /**
   * Sumo dates are encoded as longs, with no metadata that says to interpret
   * the long as a date. This table identifies those known columns
   * which are actually timestamps. Names here are in lower case, since
   * current names use inconsistent casing.
   */
  public static final Set<String> TIMESTAMP_COLUMNS = Sets.newHashSet(
      "_messagetime", "_receipttime", "executiondt", "parsequerydt");

  /**
   * Columns encoded as string, but which seem to be date/time.
   * (Although, thus far, all instances have been an empty string.)
   */
  public static final Set<String> DATETIME_COLUMNS = Sets.newHashSet(
      "buildenginedt");

  protected abstract static class FieldShim {
    protected final String fieldName;
    protected final ScalarWriter writer;

    protected FieldShim(String fieldName, ScalarWriter writer) {
      this.fieldName = fieldName;
      this.writer = writer;
    }

    protected void write(String value) {
      if (! Strings.isEmpty(value)) {
        writeValue(value);
      }
    }

    protected abstract void writeValue(String value);

    protected boolean equals(SearchJobField sumoField) {
      return fieldName.equals(sumoField.getName());
    }
  }

  private static class IntFieldShim extends FieldShim {

    protected IntFieldShim(TupleWriter writer, SearchJobField sumoField) {
      super(sumoField.getName(),
          writer.scalar(writer.addColumn(MetadataUtils.newScalar(
            sumoField.getName(),
            Types.optional(MinorType.INT)))));
    }

    @Override
    protected void writeValue(String value) {
      try {
        // Oddly, Sumo provides no intField method for int fields.
        writer.setInt(Integer.parseInt(value));
      } catch (NumberFormatException e) {
        logger.warn("Bad int value for field {}: \"{}\", Drill value will be NULL",
            fieldName, value);
      }
    }
  }

  private static class LongFieldShim extends FieldShim {

    protected LongFieldShim(TupleWriter writer, SearchJobField sumoField) {
      super(sumoField.getName(),
          writer.scalar(writer.addColumn(MetadataUtils.newScalar(
            sumoField.getName(),
            Types.optional(MinorType.BIGINT)))));
    }

    @Override
    protected void writeValue(String value) {
      try {
        writer.setLong(Long.parseLong(value));
      } catch (NumberFormatException e) {
        logger.warn("Bad long value for field {}: \"{}\", Drill value will be NULL",
            fieldName, value);
      }
    }

    @Override
    protected boolean equals(SearchJobField sumoField) {
      return super.equals(sumoField) &&
             sumoField.getFieldType().equals(SUMO_LONG_TYPE);
    }
  }

  private static class StringFieldShim extends FieldShim {

    protected StringFieldShim(TupleWriter writer, SearchJobField sumoField) {
      super(sumoField.getName(),
          writer.scalar(writer.addColumn(MetadataUtils.newScalar(
            sumoField.getName(),
            Types.optional(MinorType.VARCHAR)))));
    }

    @Override
    protected void writeValue(String value) {
      writer.setString(value);
    }

    @Override
    protected boolean equals(SearchJobField sumoField) {
      return super.equals(sumoField) &&
             sumoField.getFieldType().equals(SUMO_STRING_TYPE);
    }
  }

  private static class TimestampFieldShim extends FieldShim {

    protected TimestampFieldShim(TupleWriter writer, SearchJobField sumoField) {
      super(sumoField.getName(),
          writer.scalar(writer.addColumn(MetadataUtils.newScalar(
            sumoField.getName(),
            Types.optional(MinorType.TIMESTAMP)))));
    }

    @Override
    protected void writeValue(String value) {
      try {
        // Do long parsing here to avoid another field map lookup.
        // TODO: Not clear if timesstamp is in the requested TZ or in UTC.
        // Drill requires dates be in the TZ of the server (oddly).

        writer.setLong(Long.parseLong(value));
      } catch (NumberFormatException e) {
        logger.warn("Bad timestamp value for field {}: \"{}\", Drill value will be NULL",
            fieldName, value);
      }
    }

    @Override
    protected boolean equals(SearchJobField sumoField) {
      return super.equals(sumoField) &&
             sumoField.getFieldType().equals(SUMO_LONG_TYPE);
    }
  }

  protected final SumoBatchReader batchReader;
  protected int messageOffset;
  protected List<FieldShim> batchSchema = new ArrayList<>();
  private final Map<String, FieldShim> fieldIndex = new HashMap<>();
  private List<SearchJobField> priorSumoFields;

  public SumoReader(SumoBatchReader batchReader) {
    this.batchReader = batchReader;
  }

  protected abstract boolean next(RowSetLoader writer);

  protected void defineMessageSchema(TupleWriter writer, List<SearchJobField> sumoFields) {
    if (isSameSchema(sumoFields)) {
      return;
    }
    if (sumoFields.size() == 0) {
      throw UserException.dataReadError()
        .message("Sumo search job returned no fields")
        .addContext(batchReader.errorContext())
        .build(logger);
    }
    RequestedTuple projection = batchReader.rootProjection;
    batchSchema.clear();
    for (SearchJobField sumoField : sumoFields) {
      String fieldName = sumoField.getName();
      if (!projection.isProjected(fieldName)) {
        continue;
      }
      FieldShim fieldWriter = fieldIndex.get(fieldName);
      if (fieldWriter == null) {
        switch (sumoField.getFieldType()) {
        case SUMO_STRING_TYPE:
          fieldWriter = new StringFieldShim(writer, sumoField);
          break;
        case SUMO_LONG_TYPE:
          if (isTimestamp(sumoField)) {
            fieldWriter = new TimestampFieldShim(writer, sumoField);
          } else {
            fieldWriter = new LongFieldShim(writer, sumoField);
          }
          break;
        case SUMO_INT_TYPE:
          fieldWriter = new IntFieldShim(writer, sumoField);
          break;
        default:
          logger.warn("Unexpected Sumo field type of {} for field {}, assuming String",
              sumoField.getFieldType(), fieldName);
          fieldWriter = new StringFieldShim(writer, sumoField);
        }
      }
      batchSchema.add(fieldWriter);
    }
    priorSumoFields = sumoFields;
  }

  /**
   * Crude check if the new schema is the same as the previous. Not even sure if the
   * schema can change between messages. If it does, we really just want to check if
   * new fields have appeared that are in the desired projection list.
   */
  private boolean isSameSchema(List<SearchJobField> sumoFields) {
    if (priorSumoFields == null) {
      return false;
    }
    if (priorSumoFields.size() != sumoFields.size()) {
      return false;
    }
    for (int i = 0; i < priorSumoFields.size(); i++) {
      SearchJobField prior = priorSumoFields.get(i);
      SearchJobField current = sumoFields.get(i);
      if (!prior.getName().contentEquals(current.getName())) {
        return false;
      }
      if (!prior.getFieldType().contentEquals(current.getFieldType())) {
        return false;
      }
    }
    return true;
  }

  protected boolean isTimestamp(SearchJobField sumoField) {
    return TIMESTAMP_COLUMNS.contains(sumoField.getName().toLowerCase());
  }
}
