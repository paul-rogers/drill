package org.apache.drill.exec.store.revised;

import org.apache.drill.exec.store.revised.Sketch.*;
import org.apache.drill.exec.store.revised.BaseImpl.*;

public class ExtensionBuilder {

  private String schemaName;
  private LogicalSchema rootSchema;

  public ExtensionBuilder(String schemaName) {
    this.schemaName = schemaName;
  }

  public ExtensionBuilder rootSchema(LogicalSchema rootSchema) {
    this.rootSchema = rootSchema;
    return this;
  }

  public StorageExtension build( ) {
    SchemaReader reader = buildSchemaReader();
    LogicalSchema rootSchema = buildRootSchema(schemaName, reader, null);
    return buildExtension(rootSchema);
  }

  public StorageExtension buildExtension(LogicalSchema rootSchema) {
    return new BaseExtension(rootSchema);
  }

  public LogicalSchema buildRootSchema(String schemaName, SchemaReader reader, SchemaWriter writer) {
    return new AbstractLogicalSchema(schemaName, reader, writer);
  }

  private SchemaReader buildSchemaReader() {
    // TODO Auto-generated method stub
    return null;
  }
}
