package org.apache.drill.exec.store.log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.shaded.guava.com.google.common.base.Strings;
import org.apache.hadoop.mapred.FileSplit;

public class LogBatchReader implements ManagedReader<FileSchemaNegotiator> {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LogBatchReader.class);
  private static final String RAW_LINE_COL_NAME = "_raw";
  private static final String UNMATCHED_LINE_COL_NAME = "_unmatched_rows";

  private abstract static class ColumnDefn {
    private final String name;
    private final int index;
    private final String format;
    protected ScalarWriter colWriter;

    public ColumnDefn(String name, int index) {
      this(name, index, null);
    }

    public ColumnDefn(String name, int index, String format) {
      this.name = name;
      this.index = index;
      this.format = format;
    }

    public abstract void define(SchemaBuilder builder);

    public void bind(TupleWriter rowWriter) {
      colWriter = rowWriter.scalar(index);
    }

    public abstract void load(String value);

    public String getName() { return this.name; }

    public int getIndex() { return this.index; }

    public String getFormat() { return this.format;}

    @Override
    //For testing
    public String toString() {
      return "Name: " + name + ", Index: " + index + ", Format: " + format;
    }
  }

  private static class VarCharDefn extends ColumnDefn {

    public VarCharDefn(String name, int index) {
      super(name, index);
    }

    @Override
    public void define(SchemaBuilder builder) {
      builder.addNullable(getName(), MinorType.VARCHAR);
    }

    @Override
    public void load(String value) {
      colWriter.setString(value);
    }
  }

  private static class BigIntDefn extends ColumnDefn {

    public BigIntDefn(String name, int index) {
      super(name, index);
    }

    @Override
    public void define(SchemaBuilder builder) {
      builder.addNullable(getName(), MinorType.BIGINT);
    }

    @Override
    public void load(String value) {
      try {
        colWriter.setLong(Long.parseLong(value));
      } catch (NumberFormatException e) {
        throw UserException
            .dataReadError(e)
            .addContext("Failed to parse an INT field")
            .addContext("Column", getName())
            .addContext("Position", getIndex())
            .addContext("Value", value)
            .build(logger);
      }
    }
  }

  private static class SmallIntDefn extends ColumnDefn {

    public SmallIntDefn(String name, int index) {
      super(name, index);
    }

    @Override
    public void define(SchemaBuilder builder) {
      builder.addNullable(getName(), MinorType.SMALLINT);
    }

    @Override
    public void load(String value) {
      try {
        colWriter.setInt(Short.parseShort(value));
      } catch (NumberFormatException e) {
        throw UserException
            .dataReadError(e)
            .addContext("Failed to parse an INT field")
            .addContext("Column", getName())
            .addContext("Position", getIndex())
            .addContext("Value", value)
            .build(logger);
      }
    }
  }

  private static class IntDefn extends ColumnDefn {

    public IntDefn(String name, int index) {
      super(name, index);
    }

    @Override
    public void define(SchemaBuilder builder) {
      builder.addNullable(getName(), MinorType.INT);
    }

    @Override
    public void load(String value) {
      try {
        colWriter.setInt(Integer.parseInt(value));
      } catch (NumberFormatException e) {
        throw UserException
            .dataReadError(e)
            .addContext("Failed to parse an INT field")
            .addContext("Column", getName())
            .addContext("Position", getIndex())
            .addContext("Value", value)
            .build(logger);
      }
    }
  }

  private static class Float4Defn extends ColumnDefn {

    public Float4Defn(String name, int index) {
      super(name, index);
    }

    @Override
    public void define(SchemaBuilder builder) {
      builder.addNullable(getName(), MinorType.FLOAT4);
    }

    @Override
    public void load(String value) {
      try {
        colWriter.setDouble(Float.parseFloat(value));
      } catch (NumberFormatException e) {
        throw UserException
            .dataReadError(e)
            .addContext("Failed to parse an FLOAT field")
            .addContext("Column", getName())
            .addContext("Position", getIndex())
            .addContext("Value", value)
            .build(logger);
      }
    }
  }

  private static class DoubleDefn extends ColumnDefn {

    public DoubleDefn(String name, int index) {
      super(name, index);
    }

    @Override
    public void define(SchemaBuilder builder) {
      builder.addNullable(getName(), MinorType.FLOAT8);
    }

    @Override
    public void load(String value) {
      try {
        colWriter.setDouble(Double.parseDouble(value));
      } catch (NumberFormatException e) {
        throw UserException
            .dataReadError(e)
            .addContext("Failed to parse an FLOAT field")
            .addContext("Column", getName())
            .addContext("Position", getIndex())
            .addContext("Value", value)
            .build(logger);
      }
    }
  }

  private static class DateDefn extends ColumnDefn {

    private SimpleDateFormat df;

    public DateDefn(String name, int index, String dateFormat) {
      super(name, index, dateFormat);
      df = getValidDateObject(dateFormat);
    }

    private SimpleDateFormat getValidDateObject(String d) {
      SimpleDateFormat tempDateFormat;
      if (d != null && !d.isEmpty()) {
        tempDateFormat = new SimpleDateFormat(d);
      } else {
        throw UserException.parseError()
            .message("Invalid date format.  The date formatting string was empty.")
            .build(logger);
      }
      return tempDateFormat;
    }

    @Override
    public void define(SchemaBuilder builder) {
      builder.addNullable(getName(), MinorType.DATE);
    }

    @Override
    public void load(String value) {
      try {
        Date d = df.parse(value);
        long milliseconds = d.getTime();
        colWriter.setLong(milliseconds);
      } catch (NumberFormatException e) {
        throw UserException
            .dataReadError(e)
            .addContext("Failed to parse an DATE field")
            .addContext("Column", getName())
            .addContext("Position", getIndex())
            .addContext("Value", value)
            .build(logger);
      } catch (ParseException e) {
        throw UserException
            .dataReadError(e)
            .addContext("Date Format String does not match field value.")
            .addContext("Column", getName())
            .addContext("Position", getIndex())
            .addContext("Format String", getFormat())
            .addContext("Value", value)
            .build(logger);
      }
    }
  }

  private static class TimeDefn extends ColumnDefn {

    private SimpleDateFormat df;

    public TimeDefn(String name, int index, String dateFormat) {
      super(name, index, dateFormat);
      df = getValidDateObject(dateFormat);
    }

    private SimpleDateFormat getValidDateObject(String d) {
      SimpleDateFormat tempDateFormat;
      if (d != null && !d.isEmpty()) {
        tempDateFormat = new SimpleDateFormat(d);
      } else {
        throw UserException.parseError()
            .message("Invalid date format.  The date formatting string was empty.")
            .build(logger);
      }
      return tempDateFormat;
    }

    @Override
    public void define(SchemaBuilder builder) {
      builder.addNullable(getName(), MinorType.TIME);
    }

    @Override
    public void load(String value) {
      try {
        Date d = df.parse(value);
        int milliseconds = (int) d.getTime();
        colWriter.setLong(milliseconds);
      } catch (NumberFormatException e) {
        throw UserException
            .dataReadError(e)
            .addContext("Failed to parse an Time field")
            .addContext("Column", getName())
            .addContext("Position", getIndex())
            .addContext("Value", value)
            .build(logger);
      } catch (ParseException e) {
        throw UserException
            .dataReadError(e)
            .addContext("Date Format String does not match field value.")
            .addContext("Column", getName())
            .addContext("Position", getIndex())
            .addContext("Format String", getFormat())
            .addContext("Value", value)
            .build(logger);
      }
    }
  }

  private static class TimeStampDefn extends ColumnDefn {

    private SimpleDateFormat df;

    public TimeStampDefn(String name, int index, String dateFormat) {
      super(name, index, dateFormat);
      df = getValidDateObject(dateFormat);
    }

    private SimpleDateFormat getValidDateObject(String d) {
      SimpleDateFormat tempDateFormat;
      if (d != null && !d.isEmpty()) {
        tempDateFormat = new SimpleDateFormat(d);
      } else {
        throw UserException.parseError()
            .message("Invalid date format.  The date formatting string was empty.")
            .build(logger);
      }
      return tempDateFormat;
    }

    @Override
    public void define(SchemaBuilder builder) {
      builder.addNullable(getName(), MinorType.TIMESTAMP);
    }

    @Override
    public void load(String value) {
      try {
        Date d = df.parse(value);
        long milliseconds = d.getTime();
        colWriter.setLong(milliseconds);
      } catch (NumberFormatException e) {
        throw UserException
            .dataReadError(e)
            .addContext("Failed to parse a TIMESTAMP field")
            .addContext("Column", getName())
            .addContext("Position", getIndex())
            .addContext("Value", value)
            .build(logger);
      } catch (ParseException e) {
        throw UserException
            .dataReadError(e)
            .addContext("Date Format String does not match field value.")
            .addContext("Column", getName())
            .addContext("Position", getIndex())
            .addContext("Format String", getFormat())
            .addContext("Value", value)
            .build(logger);
      }
    }
  }

  private final FileSplit split;
  private final LogFormatConfig formatConfig;
  private ColumnDefn columns[];
  private Pattern pattern;
  private BufferedReader reader;
  private int capturingGroups;
  private ResultSetLoader loader;
  private ScalarWriter rawColWriter;
  private ScalarWriter unmatchedColWriter;
  private boolean saveMatchedRows;
  private int maxErrors;
  private int lineNumber;
  private int errorCount;

  public LogBatchReader(FileSplit split,
                         LogFormatConfig formatConfig) {
    this.split = split;
    this.formatConfig = formatConfig;
    this.maxErrors = Math.max(0, formatConfig.getMaxErrors());
  }

  @Override
  public boolean open(FileSchemaNegotiator negotiator) {
    setupPattern();
    negotiator.setTableSchema(defineSchema(), true);
    loader = negotiator.build();
    bindColumns(loader.writer());
    openFile(negotiator);
    return true;
  }

  private void setupPattern() {
    try {
      this.pattern = Pattern.compile(this.formatConfig.getRegex());
      Matcher m = pattern.matcher("test");
      capturingGroups = m.groupCount();
    } catch (PatternSyntaxException e) {
      throw UserException
          .validationError(e)
          .message("Failed to parse regex: \"%s\"", formatConfig.getRegex())
          .build(logger);
    }
  }

  private TupleMetadata defineSchema() {
    List<String> fields = formatConfig.getFieldNames();
    for (int i = fields.size(); i < capturingGroups; i++) {
      fields.add("field_" + i);
    }
    columns = new ColumnDefn[capturingGroups];
    SchemaBuilder builder = new SchemaBuilder();
    for (int i = 0; i < capturingGroups; i++) {
      columns[i] = makeColumn(fields.get(i), i);
      columns[i].define(builder);
    }
    builder.addNullable(RAW_LINE_COL_NAME, MinorType.VARCHAR);
    builder.addNullable(UNMATCHED_LINE_COL_NAME, MinorType.VARCHAR);
    TupleMetadata schema = builder.buildSchema();

    // Exclude special columns from wildcard expansion

    schema.metadata(RAW_LINE_COL_NAME).setBooleanProperty(
        ColumnMetadata.EXCLUDE_FROM_WILDCARD, true);
    schema.metadata(UNMATCHED_LINE_COL_NAME).setBooleanProperty(
        ColumnMetadata.EXCLUDE_FROM_WILDCARD, true);

    return schema;
  }

  private void bindColumns(RowSetLoader writer) {
    for (int i = 0; i < capturingGroups; i++) {
      columns[i].bind(writer);
      saveMatchedRows |= columns[i].colWriter.isProjected();
    }
    rawColWriter = writer.scalar(RAW_LINE_COL_NAME);
    saveMatchedRows |= rawColWriter.isProjected();
    unmatchedColWriter = writer.scalar(UNMATCHED_LINE_COL_NAME);

    // If no match-case columns are projected, and the unmatched
    // columns is unprojected, then we want to count (matched)
    // rows.

    saveMatchedRows |= !unmatchedColWriter.isProjected();
  }

  private ColumnDefn makeColumn(String name, int patternIndex) {
    String typeName = formatConfig.getDataType(patternIndex);
    MinorType type;
    if (Strings.isNullOrEmpty(typeName)) {
      // No type name. VARCHAR is a safe guess
      type = MinorType.VARCHAR;
    } else {
      type = MinorType.valueOf(typeName.toUpperCase());
    }

   //System.out.println( "Type name: "  + typeName + " Type: " + type);
    switch (type) {
      case VARCHAR:
        return new VarCharDefn(name, patternIndex);
      case INT:
        return new IntDefn(name, patternIndex);
      case SMALLINT:
        return new SmallIntDefn(name, patternIndex);
      case BIGINT:
        return new BigIntDefn(name, patternIndex);
      case FLOAT4:
        return new Float4Defn(name, patternIndex);
      case FLOAT8:
        return new DoubleDefn(name, patternIndex);
      case DATE:
        return new DateDefn(name, patternIndex, formatConfig.getDateFormat(patternIndex));
      case TIMESTAMP:
        return new TimeStampDefn(name, patternIndex, formatConfig.getDateFormat(patternIndex));
      case TIME:
        return new TimeDefn(name, patternIndex, formatConfig.getDateFormat(patternIndex));
      default:
        throw UserException
            .validationError()
            .message("Undefined column types")
            .addContext("Position", patternIndex)
            .addContext("Field name", name)
            .addContext("Type", typeName)
            .build(logger);
    }
  }

  private void openFile(FileSchemaNegotiator negotiator) {
    InputStream in;
    try {
      in = negotiator.fileSystem().open(split.getPath());
    } catch (Exception e) {
      throw UserException
          .dataReadError(e)
          .message("Failed to open open input file: %s", split.getPath())
          .addContext("User name", negotiator.userName())
          .build(logger);
    }
    reader = new BufferedReader(new InputStreamReader(in, Charsets.UTF_8));
  }

  @Override
  public boolean next() {
    RowSetLoader rowWriter = loader.writer();
    while (! rowWriter.isFull()) {
      if (! nextLine(rowWriter)) {
        return false;
      }
    }
    return true;
  }

  private boolean nextLine(RowSetLoader rowWriter) {
    String line;
    try {
      line = reader.readLine();
    } catch (IOException e) {
      throw UserException
          .dataReadError(e)
          .message("Error reading file:")
          .addContext("File", split.getPath())
          .build(logger);
    }

    if (line == null) {
      return false;
    }
    lineNumber++;
    Matcher lineMatcher = pattern.matcher(line);
    if (lineMatcher.matches()) {

      // Load matched row into vectors.

      if (saveMatchedRows) {
        rowWriter.start();
        rawColWriter.setString(line);
        loadVectors(lineMatcher);
        rowWriter.save();
      }
      return true;
    }

    errorCount++;
    if (errorCount < maxErrors) {
      logger.warn("Unmatached line: {}", line);
    } else {
      throw UserException.parseError()
          .message("Too many errors.  Max error threshold exceeded.")
          .addContext("Line", line)
          .addContext("Line number", lineNumber)
          .build(logger);
    }

    // For unmatched columns, create an output row only if the
    // user asked for the unmatched values.

    if (unmatchedColWriter.isProjected()) {
      rowWriter.start();
      unmatchedColWriter.setString(line);
      rowWriter.save();
    }
    return true;
  }

  private void loadVectors(Matcher m) {
    for (int i = 0; i < columns.length; i++) {
      String value = m.group(columns[i].index + 1);
      if (value != null) {
        columns[i].load(value);
      }
    }
  }

  @Override
  public void close() {
    if (reader == null) {
      return;
    }
    try {
      reader.close();
    } catch (IOException e) {
      logger.warn("Error when closing file: " + split.getPath(), e);
    } finally {
      reader = null;
    }
  }

  @Override
  public String toString() {
    return String.format(
        "LogRecordReader[File=%s, Line=%d]",
        split.getPath(), lineNumber);
  }
}
