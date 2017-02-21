/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.parquet.columnreaders;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.parquet.ParquetReaderStats;
import org.apache.drill.exec.store.parquet.ParquetReaderUtility;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.PrimitiveType;

import com.google.common.collect.Lists;

public class ParquetRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetRecordReader.class);

  // this value has been inflated to read in multiple value vectors at once, and then break them up into smaller vectors
  private static final int NUMBER_OF_VECTORS = 1;
  private static final long DEFAULT_BATCH_LENGTH = 256 * 1024 * NUMBER_OF_VECTORS; // 256kb
  private static final long DEFAULT_BATCH_LENGTH_IN_BITS = DEFAULT_BATCH_LENGTH * 8; // 256kb
  private static final char DEFAULT_RECORDS_TO_READ_IF_NOT_FIXED_WIDTH = 32*1024;
  private static final int DEFAULT_RECORDS_TO_READ_IF_VARIABLE_WIDTH = 65535;
  private static final int NUM_RECORDS_TO_READ_NOT_SPECIFIED = -1;

  // When no column is required by the downstream operator, ask SCAN to return a DEFAULT column. If such column does not exist,
  // it will return as a nullable-int column. If that column happens to exist, return that column.
  protected static final List<SchemaPath> DEFAULT_COLS_TO_READ = ImmutableList.of(SchemaPath.getSimplePath("_DEFAULT_COL_TO_READ_"));

  // TODO - should probably find a smarter way to set this, currently 1 megabyte
  public static final int PARQUET_PAGE_MAX_SIZE = 1024 * 1024 * 1;

  // used for clearing the last n bits of a byte
  public static final byte[] endBitMasks = {-2, -4, -8, -16, -32, -64, -128};
  // used for clearing the first n bits of a byte
  public static final byte[] startBitMasks = {127, 63, 31, 15, 7, 3, 1};

  private int recordsPerBatch;
  private OperatorContext operatorContext;
//  private long totalRecords;
//  private long rowGroupOffset;

  private FileSystem fileSystem;
  private long batchSize;
  private long numRecordsToRead; // number of records to read

  Path hadoopPath;
  private VarLenBinaryReader varLengthReader;
  private ParquetMetadata footer;
  // For columns not found in the file, we need to return a schema element with the correct number of values
  // at that position in the schema. Currently this requires a vector be present. Here is a list of all of these vectors
  // that need only have their value count set at the end of each call to next(), as the values default to null.
  private List<NullableIntVector> nullFilledVectors;
  // Keeps track of the number of records returned in the case where only columns outside of the file were selected.
  // No actual data needs to be read out of the file, we only need to return batches until we have 'read' the number of
  // records specified in the row group metadata
  long mockRecordsRead;

  private final CodecFactory codecFactory;
  int rowGroupIndex;
  long totalRecordsRead;
  private final FragmentContext fragmentContext;
  ParquetReaderUtility.DateCorruptionStatus dateCorruptionStatus;

  ParquetSchema schema;

  public boolean useAsyncColReader;
  public boolean useAsyncPageReader;
  public boolean useBufferedReader;
  public int bufferedReadSize;
  public boolean useFadvise;
  public boolean enforceTotalSize;
  public long readQueueSize;

  @SuppressWarnings("unused")
  private String name;

  public ParquetReaderStats parquetReaderStats = new ParquetReaderStats();

  public enum Metric implements MetricDef {
    NUM_DICT_PAGE_LOADS,         // Number of dictionary pages read
    NUM_DATA_PAGE_lOADS,         // Number of data pages read
    NUM_DATA_PAGES_DECODED,      // Number of data pages decoded
    NUM_DICT_PAGES_DECOMPRESSED, // Number of dictionary pages decompressed
    NUM_DATA_PAGES_DECOMPRESSED, // Number of data pages decompressed
    TOTAL_DICT_PAGE_READ_BYTES,  // Total bytes read from disk for dictionary pages
    TOTAL_DATA_PAGE_READ_BYTES,  // Total bytes read from disk for data pages
    TOTAL_DICT_DECOMPRESSED_BYTES, // Total bytes decompressed for dictionary pages (same as compressed bytes on disk)
    TOTAL_DATA_DECOMPRESSED_BYTES, // Total bytes decompressed for data pages (same as compressed bytes on disk)
    TIME_DICT_PAGE_LOADS,          // Time in nanos in reading dictionary pages from disk
    TIME_DATA_PAGE_LOADS,          // Time in nanos in reading data pages from disk
    TIME_DATA_PAGE_DECODE,         // Time in nanos in decoding data pages
    TIME_DICT_PAGE_DECODE,         // Time in nanos in decoding dictionary pages
    TIME_DICT_PAGES_DECOMPRESSED,  // Time in nanos in decompressing dictionary pages
    TIME_DATA_PAGES_DECOMPRESSED,  // Time in nanos in decompressing data pages
    TIME_DISK_SCAN_WAIT,           // Time in nanos spent in waiting for an async disk read to complete
    TIME_DISK_SCAN,                // Time in nanos spent in reading data from disk.
    TIME_FIXEDCOLUMN_READ,         // Time in nanos spent in converting fixed width data to value vectors
    TIME_VARCOLUMN_READ,           // Time in nanos spent in converting varwidth data to value vectors
    TIME_PROCESS;                  // Time in nanos spent in processing

    @Override public int metricId() {
      return ordinal();
    }
  }

  public ParquetRecordReader(FragmentContext fragmentContext,
      String path,
      int rowGroupIndex,
      long numRecordsToRead,
      FileSystem fs,
      CodecFactory codecFactory,
      ParquetMetadata footer,
      List<SchemaPath> columns,
      ParquetReaderUtility.DateCorruptionStatus dateCorruptionStatus) throws ExecutionSetupException {
    this(fragmentContext, DEFAULT_BATCH_LENGTH_IN_BITS, numRecordsToRead,
         path, rowGroupIndex, fs, codecFactory, footer, columns, dateCorruptionStatus);
  }

  public ParquetRecordReader(FragmentContext fragmentContext,
      String path,
      int rowGroupIndex,
      FileSystem fs,
      CodecFactory codecFactory,
      ParquetMetadata footer,
      List<SchemaPath> columns,
      ParquetReaderUtility.DateCorruptionStatus dateCorruptionStatus)
      throws ExecutionSetupException {
      this(fragmentContext, DEFAULT_BATCH_LENGTH_IN_BITS, footer.getBlocks().get(rowGroupIndex).getRowCount(),
           path, rowGroupIndex, fs, codecFactory, footer, columns, dateCorruptionStatus);
  }

  public ParquetRecordReader(
      FragmentContext fragmentContext,
      long batchSize,
      long numRecordsToRead,
      String path,
      int rowGroupIndex,
      FileSystem fs,
      CodecFactory codecFactory,
      ParquetMetadata footer,
      List<SchemaPath> columns,
      ParquetReaderUtility.DateCorruptionStatus dateCorruptionStatus) throws ExecutionSetupException {
    this.name = path;
    this.hadoopPath = new Path(path);
    this.fileSystem = fs;
    this.codecFactory = codecFactory;
    this.rowGroupIndex = rowGroupIndex;
    this.batchSize = batchSize;
    this.footer = footer;
    this.dateCorruptionStatus = dateCorruptionStatus;
    this.fragmentContext = fragmentContext;
    // Callers can pass -1 if they want to read all rows.
    if (numRecordsToRead == NUM_RECORDS_TO_READ_NOT_SPECIFIED) {
      this.numRecordsToRead = footer.getBlocks().get(rowGroupIndex).getRowCount();
    } else {
      assert (numRecordsToRead >= 0);
      this.numRecordsToRead = Math.min(numRecordsToRead, footer.getBlocks().get(rowGroupIndex).getRowCount());
    }
    useAsyncColReader =
        fragmentContext.getOptions().getOption(ExecConstants.PARQUET_COLUMNREADER_ASYNC).bool_val;
    useAsyncPageReader =
        fragmentContext.getOptions().getOption(ExecConstants.PARQUET_PAGEREADER_ASYNC).bool_val;
    useBufferedReader =
        fragmentContext.getOptions().getOption(ExecConstants.PARQUET_PAGEREADER_USE_BUFFERED_READ).bool_val;
    bufferedReadSize =
        fragmentContext.getOptions().getOption(ExecConstants.PARQUET_PAGEREADER_BUFFER_SIZE).num_val.intValue();
    useFadvise =
        fragmentContext.getOptions().getOption(ExecConstants.PARQUET_PAGEREADER_USE_FADVISE).bool_val;
    readQueueSize =
        fragmentContext.getOptions().getOption(ExecConstants.PARQUET_PAGEREADER_QUEUE_SIZE).num_val;
    enforceTotalSize =
        fragmentContext.getOptions().getOption(ExecConstants.PARQUET_PAGEREADER_ENFORCETOTALSIZE).bool_val;

    setColumns(columns);
  }

  /**
   * Flag indicating if the old non-standard data format appears
   * in this file, see DRILL-4203.
   *
   * @return true if the dates are corrupted and need to be corrected
   */
  public ParquetReaderUtility.DateCorruptionStatus getDateCorruptionStatus() {
    return dateCorruptionStatus;
  }

  public CodecFactory getCodecFactory() {
    return codecFactory;
  }

  public Path getHadoopPath() {
    return hadoopPath;
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }

  public int getRowGroupIndex() {
    return rowGroupIndex;
  }

  public int getBitWidthAllFixedFields() {
    return schema.bitWidthAllFixedFields;
  }

  public long getBatchSize() {
    return batchSize;
  }

  /**
   * @param type a fixed length type from the parquet library enum
   * @return the length in pageDataByteArray of the type
   */
  public static int getTypeLengthInBits(PrimitiveType.PrimitiveTypeName type) {
    switch (type) {
      case INT64:   return 64;
      case INT32:   return 32;
      case BOOLEAN: return 1;
      case FLOAT:   return 32;
      case DOUBLE:  return 64;
      case INT96:   return 96;
      // binary and fixed length byte array
      default:
        throw new IllegalStateException("Length cannot be determined for type " + type);
    }
  }

    public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  public FragmentContext getFragmentContext() {
    return fragmentContext;
  }

  @Override
  public void setup(OperatorContext operatorContext, OutputMutator output) throws ExecutionSetupException {
    this.operatorContext = operatorContext;
    if (isStarQuery()) {
      schema = new ParquetSchema();
    } else {
      schema = new ParquetSchema(getColumns());
      nullFilledVectors = new ArrayList<>();
    }
//    totalRecords = footer.getBlocks().get(rowGroupIndex).getRowCount();
    mockRecordsRead = 0;

//    ParquetMetadataConverter metaConverter = new ParquetMetadataConverter();
//    FileMetaData fileMetaData;

    logger.debug("Reading row group({}) with {} records in file {}.", rowGroupIndex, footer.getBlocks().get(rowGroupIndex).getRowCount(),
        hadoopPath.toUri().getPath());
    totalRecordsRead = 0;


    try {
      schema.buildSchema(footer, output);
    } catch (Exception e) {
      handleAndRaise("Failure in setting up reader", e);
    }
  }

  /**
   * Represents a single column read from the Parquet file by the record reader.
   */

  public static class ParquetColumnMetadata {

    private ColumnDescriptor column;
    private SchemaElement se;
    MaterializedField field;
    private int length;
    private MajorType type;
    ColumnChunkMetaData columnChunkMetaData;
    ValueVector vector;

    public ParquetColumnMetadata(ColumnDescriptor column) {
      this.column = column;
    }

    public void resolveDrillType(Map<String, SchemaElement> schemaElements, OptionManager options) {
      se = schemaElements.get(column.getPath()[0]);
      type = ParquetToDrillTypeConverter.toMajorType(column.getType(), se.getType_length(),
          getDataMode(column), se, options);
      field = MaterializedField.create(toFieldName(column.getPath()), type);
      length = getDataTypeLength();
    }

    private String toFieldName(String[] paths) {
      return SchemaPath.getCompoundPath(paths).getAsUnescapedPath();
    }

    private TypeProtos.DataMode getDataMode(ColumnDescriptor column) {
      if (column.getMaxRepetitionLevel() > 0 ) {
        return DataMode.REPEATED;
      } else if (column.getMaxDefinitionLevel() == 0) {
        return TypeProtos.DataMode.REQUIRED;
      } else {
        return TypeProtos.DataMode.OPTIONAL;
      }
    }

    /**
     * Returns data type length for a given {@see ColumnDescriptor} and it's corresponding
     * {@see SchemaElement}. Neither is enough information alone as the max
     * repetition level (indicating if it is an array type) is in the ColumnDescriptor and
     * the length of a fixed width field is stored at the schema level.
     *
     * @return the length if fixed width, else -1
     */
    private int getDataTypeLength() {
      if (isFixedLength()) {
        if (column.getMaxRepetitionLevel() > 0) {
          return -1;
        }
        if (column.getType() == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
          return se.getType_length() * 8;
        } else {
          return getTypeLengthInBits(column.getType());
        }
      } else {
        return -1;
      }
    }

    public boolean isFixedLength( ) {
      return column.getType() != PrimitiveType.PrimitiveTypeName.BINARY;
    }

    public boolean isRepeated() {
      return column.getMaxRepetitionLevel() > 0;
    }

    private ValueVector buildVector(OutputMutator output) throws SchemaChangeException {
      Class<? extends ValueVector> vectorClass = TypeHelper.getValueVectorClass(type.getMinorType(), type.getMode());
      vector = output.addField(field, vectorClass);
      return vector;
    }

    private ColumnReader<?> makeFixedWidthReader(ParquetRecordReader reader, int recordsPerBatch) throws Exception {
      return ColumnReaderFactory.createFixedColumnReader(reader, true,
          column, columnChunkMetaData, recordsPerBatch, vector, se);
    }

    @SuppressWarnings("resource")
    private FixedWidthRepeatedReader makeRepeatedFixedWidthReader(ParquetRecordReader reader, int recordsPerBatch) throws Exception {
      final RepeatedValueVector repeatedVector = RepeatedValueVector.class.cast(vector);
      ColumnReader<?> dataReader = ColumnReaderFactory.createFixedColumnReader(reader, true,
          column, columnChunkMetaData, recordsPerBatch,
          repeatedVector.getDataVector(), se);
      return new FixedWidthRepeatedReader(reader, dataReader,
          getTypeLengthInBits(column.getType()), -1, column, columnChunkMetaData, false, repeatedVector, se);
    }

    private VarLengthValuesColumn<?> makeVariableWidthReader(ParquetRecordReader reader) throws ExecutionSetupException {
      return ColumnReaderFactory.getReader(reader, -1, column, columnChunkMetaData, false, vector, se);
    }

  }

  /**
   * Mapping from the schema of the Parquet file to that of the record reader.
   */

  public class ParquetSchema {
    Collection<SchemaPath> selectedCols;
    // This is a parallel list to the columns list above, it is used to determine the subset of the project
    // pushdown columns that do not appear in this file
    private boolean[] columnsFound;
    ParquetMetadata footer;
    Map<String, SchemaElement> schemaElements;
    int columnsToScan;
    private List<ColumnDescriptor> columns;
    private List<ParquetColumnMetadata> columnMd = new ArrayList<>();
    private int bitWidthAllFixedFields;
    private boolean allFieldsFixedLength = true;
    private List<ColumnReader<?>> columnStatuses = new ArrayList<>();

    public ParquetSchema() {
      selectedCols = null;
    }

    public ParquetSchema(Collection<SchemaPath> selectedCols) {
      this.selectedCols = selectedCols;
      columnsFound = new boolean[selectedCols.size()];
    }

    public void buildSchema(ParquetMetadata footer, OutputMutator output) throws Exception {
      this.footer = footer;
      columns = footer.getFileMetaData().getSchema().getColumns();
      loadParquetSchema();
      computeFixedPart();
//    rowGroupOffset = footer.getBlocks().get(rowGroupIndex).getColumns().get(0).getFirstDataPageOffset();

      if (columnsToScan != 0  && allFieldsFixedLength) {
        recordsPerBatch = (int) Math.min(Math.min(batchSize / bitWidthAllFixedFields,
            footer.getBlocks().get(0).getColumns().get(0).getValueCount()), DEFAULT_RECORDS_TO_READ_IF_VARIABLE_WIDTH);
      }
      else {
        recordsPerBatch = DEFAULT_RECORDS_TO_READ_IF_NOT_FIXED_WIDTH;
      }
      buildVectors(ParquetRecordReader.this, output);
      if (! isStarQuery()) {
        createNonExistentColumns(output);
      }
   }

    private void loadParquetSchema() {
      // TODO - figure out how to deal with this better once we add nested reading, note also look where this map is used below
      // store a map from column name to converted types if they are non-null
      schemaElements = ParquetReaderUtility.getColNameToSchemaElementMapping(footer);
      OptionManager options = fragmentContext.getOptions();

      // loop to add up the length of the fixed width columns and build the schema
      for (ColumnDescriptor column : columns) {
        ParquetColumnMetadata colMd = new ParquetColumnMetadata(column);
        colMd.resolveDrillType(schemaElements, options);
        if (! fieldSelected(colMd.field)) {
          continue;
        }
        columnMd.add(colMd);
        columnsToScan++;
      }
    }

    private void computeFixedPart() {
      for (ParquetColumnMetadata colMd : columnMd) {
        if (colMd.isFixedLength()) {
          bitWidthAllFixedFields += colMd.length;
        } else {
          allFieldsFixedLength = false;
        }
      }
    }

    public boolean isStarQuery() { return selectedCols == null; }

    private boolean fieldSelected(MaterializedField field) {
      // TODO - not sure if this is how we want to represent this
      // for now it makes the existing tests pass, simply selecting
      // all available data if no columns are provided
      if (isStarQuery()) {
        return true;
      }

      int i = 0;
      for (SchemaPath expr : selectedCols) {
        if ( field.getPath().equalsIgnoreCase(expr.getAsUnescapedPath())) {
          columnsFound[i] = true;
          return true;
        }
        i++;
      }
      return false;
    }

    @SuppressWarnings("unchecked")
    public void buildVectors(ParquetRecordReader reader, OutputMutator output) throws Exception {
      final ArrayList<VarLengthColumn<? extends ValueVector>> varLengthColumns = new ArrayList<>();
      // initialize all of the column read status objects
      BlockMetaData rowGroupMetadata = footer.getBlocks().get(rowGroupIndex);
      Map<String, Integer> columnChunkMetadataPositionsInList = buildChunkMap(rowGroupMetadata);
      for (ParquetColumnMetadata colMd : columnMd) {
        ColumnDescriptor column = colMd.column;
        colMd.columnChunkMetaData = rowGroupMetadata.getColumns().get(columnChunkMetadataPositionsInList.get(Arrays.toString(column.getPath())));
        colMd.buildVector(output);
        if (colMd.isFixedLength( )) {
          if (colMd.isRepeated()) {
             varLengthColumns.add(colMd.makeRepeatedFixedWidthReader(reader, recordsPerBatch));
          }
          else {
            columnStatuses.add(colMd.makeFixedWidthReader(reader, recordsPerBatch));
          }
        } else {
          // create a reader and add it to the appropriate list
          varLengthColumns.add(colMd.makeVariableWidthReader(reader));
        }
      }
      varLengthReader = new VarLenBinaryReader(reader, varLengthColumns);
    }

    /**
     * Create "dummy" fields for columns which are selected in the SELECT clause, but not
     * present in the Parquet schema.
     * @param output the output container
     * @throws SchemaChangeException should not occur
     */

    public void createNonExistentColumns(OutputMutator output) throws SchemaChangeException {
      List<SchemaPath> projectedColumns = Lists.newArrayList(getColumns());
      for (int i = 0; i < columnsFound.length; i++) {
        SchemaPath col = projectedColumns.get(i);
        assert col != null;
        if ( ! columnsFound[i] && ! col.equals(STAR_COLUMN)) {
          nullFilledVectors.add(createMissingColumn(col, output));
        }
      }
    }

    /**
     * Create a "dummy" column for a missing field. The column is of type optional
     * int, but will always be null.
     *
     * @param col the selected, but non-existent, schema path
     * @param output the output container
     * @return the value vector for the field
     * @throws SchemaChangeException should not occur
     */

    private NullableIntVector createMissingColumn(SchemaPath col, OutputMutator output) throws SchemaChangeException {
      MaterializedField field = MaterializedField.create(col.getAsUnescapedPath(),
                            Types.optional(TypeProtos.MinorType.INT));
      return (NullableIntVector) output.addField(field,
                TypeHelper.getValueVectorClass(TypeProtos.MinorType.INT, DataMode.OPTIONAL));
    }

    private Map<String, Integer> buildChunkMap(BlockMetaData rowGroupMetadata) {
      // the column chunk meta-data is not guaranteed to be in the same order as the columns in the schema
      // a map is constructed for fast access to the correct columnChunkMetadata to correspond
      // to an element in the schema
      Map<String, Integer> columnChunkMetadataPositionsInList = new HashMap<>();

      int colChunkIndex = 0;
      for (ColumnChunkMetaData colChunk : rowGroupMetadata.getColumns()) {
        columnChunkMetadataPositionsInList.put(Arrays.toString(colChunk.getPath().toArray()), colChunkIndex);
        colChunkIndex++;
      }
      return columnChunkMetadataPositionsInList;
    }

    public int getBitWidthAllFixedFields() {
      return bitWidthAllFixedFields;
    }

    public void close() {
      if (columnStatuses != null) {
        for (final ColumnReader<?> column : columnStatuses) {
          column.clear();
        }
        columnStatuses.clear();
        columnStatuses = null;
      }
    }
  }

  protected void handleAndRaise(String s, Exception e) {
    String message = "Error in parquet record reader.\nMessage: " + s +
      "\nParquet Metadata: " + footer;
    throw new DrillRuntimeException(message, e);
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    try {
      for (final ValueVector v : vectorMap.values()) {
        AllocationHelper.allocate(v, recordsPerBatch, 50, 10);
      }
    } catch (NullPointerException e) {
      throw new OutOfMemoryException();
    }
  }

  private void resetBatch() {
    for (final ColumnReader<?> column : schema.columnStatuses) {
      column.valuesReadInCurrentPass = 0;
    }
    for (final VarLengthColumn<?> r : varLengthReader.columns) {
      r.valuesReadInCurrentPass = 0;
    }
  }

 public void readAllFixedFields(long recordsToRead) throws IOException {
   Stopwatch timer = Stopwatch.createStarted();
   if(useAsyncColReader){
     readAllFixedFieldsParallel(recordsToRead) ;
   } else {
     readAllFixedFieldsSerial(recordsToRead);
   }
   parquetReaderStats.timeFixedColumnRead.addAndGet(timer.elapsed(TimeUnit.NANOSECONDS));
 }

  public void readAllFixedFieldsSerial(long recordsToRead) throws IOException {
    for (ColumnReader<?> crs : schema.columnStatuses) {
      crs.processPages(recordsToRead);
    }
  }

  public void readAllFixedFieldsParallel(long recordsToRead) throws IOException {
    ArrayList<Future<Long>> futures = Lists.newArrayList();
    for (ColumnReader<?> crs : schema.columnStatuses) {
      Future<Long> f = crs.processPagesAsync(recordsToRead);
      futures.add(f);
    }
    Exception exception = null;
    for(Future<Long> f: futures){
      if (exception != null) {
        f.cancel(true);
      } else {
        try {
          f.get();
        } catch (Exception e) {
          f.cancel(true);
          exception = e;
        }
      }
    }
    if (exception != null) {
      handleAndRaise(null, exception);
    }
  }

  @Override
  public int next() {
    resetBatch();
    long recordsToRead = 0;
    Stopwatch timer = Stopwatch.createStarted();
    try {
      ColumnReader<?> firstColumnStatus;
      if (schema.columnStatuses.size() > 0) {
        firstColumnStatus = schema.columnStatuses.iterator().next();
      }
      else{
        if (varLengthReader.columns.size() > 0) {
          firstColumnStatus = varLengthReader.columns.iterator().next();
        }
        else{
          firstColumnStatus = null;
        }
      }
      // No columns found in the file were selected, simply return a full batch of null records for each column requested
      if (firstColumnStatus == null) {
        if (mockRecordsRead == footer.getBlocks().get(rowGroupIndex).getRowCount()) {
          parquetReaderStats.timeProcess.addAndGet(timer.elapsed(TimeUnit.NANOSECONDS));
          return 0;
        }
        recordsToRead = Math.min(DEFAULT_RECORDS_TO_READ_IF_NOT_FIXED_WIDTH, footer.getBlocks().get(rowGroupIndex).getRowCount() - mockRecordsRead);

        // Pick the minimum of recordsToRead calculated above and numRecordsToRead (based on rowCount and limit).
        recordsToRead = Math.min(recordsToRead, numRecordsToRead);

        for (final ValueVector vv : nullFilledVectors ) {
          vv.getMutator().setValueCount( (int) recordsToRead);
        }
        mockRecordsRead += recordsToRead;
        totalRecordsRead += recordsToRead;
        numRecordsToRead -= recordsToRead;
        parquetReaderStats.timeProcess.addAndGet(timer.elapsed(TimeUnit.NANOSECONDS));
        return (int) recordsToRead;
      }

      if (schema.allFieldsFixedLength) {
        recordsToRead = Math.min(recordsPerBatch, firstColumnStatus.columnChunkMetaData.getValueCount() - firstColumnStatus.totalValuesRead);
      } else {
        recordsToRead = DEFAULT_RECORDS_TO_READ_IF_NOT_FIXED_WIDTH;
      }

      // Pick the minimum of recordsToRead calculated above and numRecordsToRead (based on rowCount and limit)
      recordsToRead = Math.min(recordsToRead, numRecordsToRead);

      if (schema.allFieldsFixedLength) {
        readAllFixedFields(recordsToRead);
      } else { // variable length columns
        long fixedRecordsToRead = varLengthReader.readFields(recordsToRead, firstColumnStatus);
        readAllFixedFields(fixedRecordsToRead);
      }

      // if we have requested columns that were not found in the file fill their vectors with null
      // (by simply setting the value counts inside of them, as they start null filled)
      if (nullFilledVectors != null) {
        for (final ValueVector vv : nullFilledVectors ) {
          vv.getMutator().setValueCount(firstColumnStatus.getRecordsReadInCurrentPass());
        }
      }

//      logger.debug("So far read {} records out of row group({}) in file '{}'", totalRecordsRead, rowGroupIndex, hadoopPath.toUri().getPath());
      totalRecordsRead += firstColumnStatus.getRecordsReadInCurrentPass();
      numRecordsToRead -= firstColumnStatus.getRecordsReadInCurrentPass();
      parquetReaderStats.timeProcess.addAndGet(timer.elapsed(TimeUnit.NANOSECONDS));

      return firstColumnStatus.getRecordsReadInCurrentPass();
    } catch (Exception e) {
      handleAndRaise("\nHadoop path: " + hadoopPath.toUri().getPath() +
        "\nTotal records read: " + totalRecordsRead +
        "\nMock records read: " + mockRecordsRead +
        "\nRecords to read: " + recordsToRead +
        "\nRow group index: " + rowGroupIndex +
        "\nRecords in row group: " + footer.getBlocks().get(rowGroupIndex).getRowCount(), e);
    }

    // this is never reached
    return 0;
  }

  @Override
  public void close() {
    logger.debug("Read {} records out of row group({}) in file '{}'", totalRecordsRead, rowGroupIndex,
        hadoopPath.toUri().getPath());
    // enable this for debugging when it is know that a whole file will be read
    // limit kills upstream operators once it has enough records, so this assert will fail
//    assert totalRecordsRead == footer.getBlocks().get(rowGroupIndex).getRowCount();
    schema.close();

    codecFactory.release();

    if (varLengthReader != null) {
      for (final VarLengthColumn<? extends ValueVector> r : varLengthReader.columns) {
        r.clear();
      }
      varLengthReader.columns.clear();
      varLengthReader = null;
    }

    if (parquetReaderStats != null) {
      updateStats();
      parquetReaderStats.logStats(logger, hadoopPath);
      parquetReaderStats = null;
    }
  }

  private void updateStats() {
    parquetReaderStats.update(operatorContext.getStats());
  }

  @Override
  protected List<SchemaPath> getDefaultColumnsToRead() {
    return DEFAULT_COLS_TO_READ;
  }
}
