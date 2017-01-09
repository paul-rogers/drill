package org.apache.drill.exec.store.revised.proto;

import java.util.List;

import org.apache.drill.exec.store.revised.ExtendableLogicalScanPop;
import org.apache.drill.exec.store.revised.Sketch.FilterExpr;
import org.apache.drill.exec.store.revised.Sketch.LogicalSchema;
import org.apache.drill.exec.store.revised.Sketch.LogicalTable;
import org.apache.drill.exec.store.revised.Sketch.RowSchema;
import org.apache.drill.exec.store.revised.Sketch.SchemaReader;
import org.apache.drill.exec.store.revised.Sketch.SchemaWriter;
import org.apache.drill.exec.store.revised.Sketch.StorageSpace;
import org.apache.drill.exec.store.revised.Sketch.TableScan;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.drill.exec.store.revised.StorageExtension;
import org.apache.drill.exec.store.revised.BaseImpl.*;

public class ProtoExtension extends BaseExtension {

  public ProtoExtension(String schemaName) {
    super(new AbstractLogicalSchema(schemaName, new ProtoSchemaReader(), null));
  }

//  public static class ProtoSchema extends AbstractLogicalSchema {
//
//    private ProtoSchemaReader reader = new ProtoSchemaReader();
//
//    public ProtoSchema(String schemaName) {
//      super(schemaName);
//    }
//
//    @Override
//    public SchemaReader reader() {
//       return reader;
//    }
//  }

  public static class ProtoSchemaReader extends AbstractSchemaReader {

    @Override
    public TableScan scan(LogicalTable table) {
      // TODO Auto-generated method stub
      return null;
    }


  }

//  @JsonTypeName("proto-table")
//  public static class ProtoTableMemento {
//
//    private String name;
//
//    public ProtoTableMemento(@JsonProperty("tableName") String tableName) {
//      name = tableName;
//    }
//
//    public String getTableName( ) { return name; }
//  }

//  public static class ProtoTable extends AbstractLogicalTable {
//
//    public ProtoTable(String name) {
//      super(name);
//    }
//  }
}
