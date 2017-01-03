package org.apache.drill.exec.store.revised;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;

public class Sketch {

//  interface RowSetBuilder {
//    RowSchema rootSchema();
//    RowSchema rowSchema();
//    RowBuilder row( );
//    boolean full( );
//    void build( );
//  }

//  interface ResultSetBuilder {
//    SchemaBuilder newSchema();
//    SchemaBuilder extendSchema();
//    SchemaBuilder reviseSchema();
//    RowSetBuilder rowSet();
//    void eof();
//  }

  public interface ScanReceiver {
    RowSetBuilder newSchema();
    RowSetReceiver rowSet();
    void close();
    int rowCount();
  }

  interface RowSetBuilder {
    SchemaBuilder schema();
    RowSetReceiver build();
  }

  public interface RowSetReceiver {
    RowSchema rootSchema();
    RowSchema rowSchema();
    RowBatchReceiver batch();
    RowSetBuilder reviseSchema();
    int rowCount();
    void close();
  }

  public interface RowBatchReceiver {
    RowBuilder row( );
    int rowCount();
    boolean full( );
    void close();
  }

  public interface RowBuilder {
    ColumnBuilder column(int index);
    ColumnBuilder column(String path);
    void accept( );
    void reject( );
  }

  public interface ColumnBuilder {
    void setString(String value);
    void setInt(int value);
    void setLong(long value);
    void setFloat(float value);
    void setDouble(double value);
    void setDecimal(BigDecimal value);
    void setNull();
    // ...
  }

  public interface SchemaBuilder {
    ColumnSchemaBuilder column(String name);
    SchemaBuilder column(String name, MajorType type);
    SchemaBuilder column(String name, MinorType type, DataMode cardinality);
    ColumnSchemaBuilder reviseColumn(String name);
    SchemaBuilder remove(String name);
    RowSchema build();
  }

  public interface ColumnSchemaBuilder {
    String name();
    ColumnSchemaBuilder type( MinorType type );
    MinorType getType();
    ColumnSchemaBuilder cardinality(DataMode mode);
    DataMode getCardinality();
    ColumnSchemaBuilder majorType( MajorType type );
    SchemaBuilder map( );
    SchemaBuilder getMapSchema( );
  }

  /**
   * Represents name space for tables referenceable in SQL statements.
   */

  interface TableSpace {
    Iterator<LogicalTable> tables( );
    LogicalTable table(String name);
    StorageSpace storage();
  }

  /**
   * Represents a model of the physical storage of a table, including
   * details of the formats available, the format for a table, the
   * partitions of the table, etc.
   */
  interface StorageSpace {
    TableScan scan(LogicalTable table);
    TableWriter create(LogicalTable table);
    TableWriter update(LogicalTable table);
  }

  enum TableCapabilities { READ, CREATE, UPDATE };

  /**
   * Represents a table visible to SQL and referenceable in the FROM
   * clause of a SELECT statement. The storage of the table can
   * be anything; this is just the view as presented to SQL.
   */

  interface LogicalTable {
    String name();
    TableCapabilities capabilites( );
    boolean staticSchema();
    RowSchema schema();
  }

  /**
   * Represents a request to scan a table. Offers the scan opportunities
   * to reduce scan cost by asking the scan if it can do a select and/or
   * filter condition internally.
   */

  interface TableScan {
    LogicalTable table();
    List<String> select( List<String> cols );
    List<FilterExpr> where( List<FilterExpr> exprs );
    FormatService format();
    Collection<TablePartition> partitions(TableScan table);
  }

  interface TableWriter {

  }

  interface FilterExpr {
    String column();
    int op();
    Object arg();
    Object[] args();
  }

  /**
   * An unit of scan work: blocks of a file, distributed query for a
   * underlying RDBMS, etc.
   */
  interface TablePartition {
    TableScan scan();
  }

  /**
   * Logical description of the schema of a row. The schema is a tuple:
   * a collection of columns with specified names and indexes. Each column
   * map be a simple data value, an array or a map, which is itself a
   * tuple in Drill.
   */

  interface RowSchema {
    int size();
    int totalSize();
    List<ColumnSchema> schema();
    ColumnSchema column(String name);
    ColumnSchema column(int index);
    RowSchema flatten();
    boolean isCaseSensitive();
  }

  /**
   * Definition of an individual column. Each has a name, a type and a
   * cardinality. If the column is a map, it has its own tuple schema.
   */

  interface ColumnSchema {
    String NAME_SEPARATOR = ".";

    int index();
    int flatIndex();
    String name();
    String path();
    MinorType type();
    DataMode cardinality();
    MajorType majorType();
    RowSchema map();
    MaterializedField materializedField();
  }

  interface FormatService {

  }

  public interface ScanService {
    ScanReceiver receiver();
  }

  public interface Deserializer {
    void bind(ScanService service);
    void open() throws Exception;
    void readBatch() throws Exception;
    void close() throws Exception;
  }
}
