package org.apache.drill.exec.store.revised;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.revised.Sketch.ScanBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Iterators;

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

  // Idea is to:
  // - Ask for current schema
  // - New batch with current schema
  // - Revise schema & get new batch
  // - New schema & get new batch

  public interface ScanReceiver {
    RowSetBuilder newSchema();
    RowSetBuilder reviseSchema();
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
    int rowCount();
    void close();
  }

  public interface RowBatchReceiver {
    RowBuilder row( );
    int rowCount();
    boolean full( );
    void close();
  }

  // Good - keep this, clear definition of a row

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
   * <p>
   */

//  public interface TableSpace {
//    String schemaName();
//    Iterable<LogicalTable> tables( );
//    LogicalTable table(String name);
//    StorageSpace storage();
//  }

  /**
   * Represents a named name space for tables referencable from SQL statements
   * in the form:<br>
   * plugin-config-name [. exten-schema-name]* . table-name
   * <p>
   * The <tt>plugin-config-name</tt> can be thought of as an alias to some table
   * name space.
   * Examples: a file system, a directory structure within a file system,
   * a logical view of some set of directories within a file system, a
   * database schema, an instance of an API, etc.
   * <p>
   * For example, suppose that you want Drill to access a database (via JDBC)
   * called <tt>foo</tt> and a directory called <tt>foo</tt>. To to this, you
   * create a storage plugin configuration for each, but must assign unique
   * aliases, perhaps "foo-db" and "foo-dir" (or "db.foo" or "dfs.foo".) Thus,
   * the root schema for an extension may have an alias known to Drill which is
   * distinct from the schema name in the underlying system. Typically, the
   * storage plugin configuration provides the underlying name as an
   * extension-specific property.
   * <p>
   * The root level construct for a schema. A schema has a name. For root-level
   * schemas defined via a storage plugin configuration, the schema name must
   * match that defined in the storage plugin configuration, as that is the name
   * that the planner will use to resolve to the logical schema. However, a
   * logical schema can contain nested schemas. (The storage plugin name might
   * resolve to a MySQL database, with the next level of schema mapping to the
   * schemas defined within that one MySQL instance.) When resolving a child
   * schema, the name is relative to the parent schema.
   * <p>
   * A schema can be readable (the most typical case), writable, or both.
   * Writable schemas allow CREATE TABLE AS (CTAS) statements, along with
   * DROP TABLE and so on.
   * <p>
   * The logical schema is assumed to exist indefinitely, but at least for the
   * (undefined) duration of the planning session that uses it.
   * <p>
   * Every extension must have at least one logical schema to represent the
   * schema defined by the storage plugin configuration. It may also have
   * as many nested child schemas as needed for the extension.
   * <p>
   * Provides read access to a schema (name space) defined by the extension. The
   * name space is defined to be occupied by a collection of zero or more tables,
   * however the extension elects to define the table. Each table must have a unique
   * name within the name space. Case sensitivity is up to the extension.
   * <p>
   * The life of a schema reader is at least for a planning session (though, at present,
   * there is no indication that the session is over.) More typically, the schema
   * reader is a facade to some underlying name space such as a directory, a DB
   * schema, an application concept, etc.
   * <p>
   * The table name is relative to this one name space: the schema name is removed.
   * The table name reported by the logical table must be the same as the one used to
   * look up the table. (Or, more generally, if the names differ, then both must resolve
   * to the same logical table. This is important because the planner will request the
   * same logical table multiple times: sometimes using the name from the SQL statement,
   * sometimes using the name provided by the logical table itself.
   */

  public interface LogicalSchema {
    LogicalSchema parent();
    String schemaName();
    QualifiedName fullName();
    LogicalSchema resolveSchema(String name);
    Iterable<LogicalTable> tables( );
    LogicalTable table(String name);
    void bind(StorageExtension extension);
    StorageExtension extension();
    TableScan scan(LogicalTable table);
  }

  public interface QualifiedName {
    String tail();
    String fullName();
    List<String> parts();
    List<String> parentName();
  }

  /**
   */

//  public interface SchemaReader {
//  }

  public interface TableInterator {
    Iterable<LogicalTable> tables(LogicalSchema schema);
  }

  public interface SchemaResolver {
    LogicalSchema resolve(LogicalSchema parent, String name);
  }

  public interface TableResolver {
    LogicalTable resolve(LogicalSchema schema, String name);
  }

  public interface TableScanCreator {
    TableScan scan(LogicalTable table);
  }

//  public interface SchemaWriter {
//
//  }

  /**
   * Represents a model of the physical storage of a table, including
   * details of the formats available, the format for a table, the
   * partitions of the table, etc.
   * <p>
   * Examples: a DB server, a file system etc.
   */
  public interface StorageSpace {
    String schemaName();
//    TableSpace tableSpace( String schemaName );
//    TableScan scan(LogicalTable table);
    TableWriter create(LogicalTable table);
    TableWriter update(LogicalTable table);
  }

  /**
   * Represents a logical table resolved through a logical schema. A logical
   * table is anything that can retrieve or consume rows. The term "logical"
   * implies that the underlying representation can be anything: a file,
   * a database table, an API or anything else. This interface forms the
   * bridge between the Drill planner and that underlying implementation.
   * <p>
   * Regardless of the implementation, SQL expects the table to be visible
   * as a simple table name. This interface represents the resolution of
   * a table name to a set of information required by the planner.
   * <p>
   * The primary purpose of a logical table is to tell the planner what
   * the table can do (readable, createable, updateable.) Also, to identify
   * if the table has an a-priori known schema, or if the schema must be
   * discovered at run time. If the schema is known, then the table can
   * report the schema for use in the planner.
   */

  public interface LogicalTable {
    int READ = 1;
    int CREATE = 2;
    int UPDATE = 4;

    LogicalSchema nameSpace();
    String name();
    int capabilites( );
    boolean staticSchema();
    RowSchema rowSchema();
    int partitionCount();
  }

  /**
   * Represents a request to scan a table, known as a "group scan" in the
   * Drill planner. Offers the scan opportunities
   * to reduce scan cost by asking the scan if it can do a select and/or
   * filter condition internally. Handles splitting the scan across Drillbits.
   * <p>
   * The Drill planner works with immutable objects copied at each step of
   * the planning process. This extension framework, however, works by
   * maintaining a single object that evolves over the planning process.
   */

//  public interface TableScanState {
//    LogicalTable table();
//    List<String> projection();
//    List<FilterExpr> where( List<FilterExpr> exprs );
////    FormatService format();
//    Collection<TablePartition> partitions(TableScan table);
//  }

  public interface TableScan {
    LogicalTable table();
    List<String> select( List<String> cols );
    List<FilterExpr> where( List<FilterExpr> exprs );
    void userName(String userName);
    String userName();
    void columns(List<SchemaPath> columns);
    int partitionCount();
    void setAssignments(List<DrillbitEndpoint> endpoints);
    boolean supportsProject();
    void buildPhysicalScans();
    SubScan getPhysicalScan(int minorFragmentIndex);
    void scanBuilder(ScanBuilder scanBuilder);
  }

  public interface ScanSelector {
    ScanBuilder select(LogicalTable table);
  }

  public interface ScanBuilder {
    boolean supportsProject();
    List<SubScan> build(TableScan scan);
  }

  public interface TableWriter {

  }

  public interface FilterExpr {
    String column();
    int op();
    Object arg();
    Object[] args();
  }

  /**
   * An unit of scan work: blocks of a file, distributed query for a
   * underlying RDBMS, etc.
   */
  public interface TablePartition {
    TableScan scan();
  }

  /**
   * Logical description of the schema of a row. The schema is a tuple:
   * a collection of columns with specified names and indexes. Each column
   * map be a simple data value, an array or a map, which is itself a
   * tuple in Drill.
   */

  public interface RowSchema {
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

  public interface ColumnSchema {
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

  public interface ScanOperation<P> {
    ScanReceiver receiver();
    ExtendablePhysicalScanPop<P> definition();
  }

  public interface Deserializer<P> {
    void bind(ScanOperation<P> service);
    void open() throws Exception;
    void readBatch() throws Exception;
    void close() throws Exception;
  }

  public static class SchemaId {
    private String extensionId;
    private String schema;

    public SchemaId(
            @JsonProperty("extensionId") String extensionId,
            @JsonProperty("schema") String schema ) {
      this.extensionId = extensionId;
      this.schema = schema;
    }

    public String getExtensionId( ) { return extensionId; }
    public String getSchema( ) { return schema; }
  }

  public interface StorageSpaceFactory {
    <T> StorageSpace create(T config);
  }
}
