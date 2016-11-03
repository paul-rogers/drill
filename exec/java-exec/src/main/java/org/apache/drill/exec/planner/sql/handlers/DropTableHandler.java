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
package org.apache.drill.exec.planner.sql.handlers;

import java.io.IOException;
import java.util.List;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.planner.sql.parser.SqlDropTable;
import org.apache.drill.exec.store.AbstractSchema;

// SqlHandler for dropping a table.
public class DropTableHandler extends DefaultSqlHandler {

  private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DropTableHandler.class);

  public DropTableHandler(SqlHandlerConfig config) {
    super(config);
  }

  /**
   * Function resolves the schema and invokes the drop method
   * (while IF EXISTS statement is used function invokes the drop method only if table exists).
   * Raises an exception if the schema is immutable.
   *
   * @param sqlNode - SqlDropTable (SQL parse tree of drop table [if exists] query)
   * @return - Single row indicating drop succeeded or table is not found while IF EXISTS statement is used,
   * raise exception otherwise
   * @throws ValidationException
   * @throws RelConversionException
   * @throws IOException
   */
  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException {
    SqlDropTable dropTableNode = ((SqlDropTable) sqlNode);
    String originalTableName = dropTableNode.getName();
    SchemaPlus defaultSchema = config.getConverter().getDefaultSchema();
    List<String> tableSchema = dropTableNode.getSchema();

    boolean removedTemporaryTable = removeTemporaryTable(tableSchema, originalTableName);
    if (!removedTemporaryTable) {
      AbstractSchema drillSchema = SchemaUtilites.resolveToMutableDrillSchema(defaultSchema, tableSchema);
      if (drillSchema == null) {
        throw UserException.validationError().message("Invalid table_name [%s]", originalTableName).build(logger);
      }

      final Table tableToDrop = SqlHandlerUtil.getTableFromSchema(drillSchema, originalTableName);
      if (tableToDrop == null || tableToDrop.getJdbcTableType() != Schema.TableType.TABLE) {
        if (dropTableNode.checkTableExistence()) {
          return DirectPlan.createDirectPlan(context, false, String.format("Table [%s] not found", originalTableName));
        } else {
          throw UserException.validationError().message("Table [%s] not found", originalTableName).build(logger);
        }
      }
      drillSchema.dropTable(originalTableName);
    }

    String message = String.format("%s [%s] dropped",
            removedTemporaryTable ? "Temporary table" : "Table", originalTableName);
    logger.info(message);
    return DirectPlan.createDirectPlan(context, true, message);
  }

  /**
   * Checks if table aimed to be dropped is temporary table.
   * If table is temporary table, it is dropped from schema
   * and removed from temporary tables session cache.
   *
   * @param tableSchema table schema indicated in drop statement
   * @param tableName   table name to drop
   * @return true if temporary table existed and was dropped, false otherwise
   */
  private boolean removeTemporaryTable(List<String> tableSchema, String tableName) {
    String defaultTemporaryWorkspace = context.getConfig().getString(ExecConstants.DEFAULT_TEMPORARY_WORKSPACE);
    String temporaryTableSchema = tableSchema.isEmpty() ?
        defaultTemporaryWorkspace : SchemaUtilites.getSchemaPath(tableSchema);
    return context.getSession().removeTemporaryTable(temporaryTableSchema, tableName);
  }
}
