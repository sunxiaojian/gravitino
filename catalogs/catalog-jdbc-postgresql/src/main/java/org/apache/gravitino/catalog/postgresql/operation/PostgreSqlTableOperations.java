/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog.postgresql.operation;

import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.DatabaseOperation;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.jdbc.operation.RequireDatabaseOperation;
import org.apache.gravitino.catalog.jdbc.utils.SqlBuilder;
import org.apache.gravitino.exceptions.NoSuchColumnException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.types.Types;

/** Table operations for PostgreSQL. */
public class PostgreSqlTableOperations extends JdbcTableOperations
    implements RequireDatabaseOperation {

  public static final String PG_QUOTE = "\"";
  private static final String NEW_LINE = "\n";
  private static final String ALTER_TABLE = "ALTER TABLE ";
  private static final String ALTER_COLUMN = "ALTER COLUMN ";
  private static final String IS = " IS '";
  private static final String COLUMN_COMMENT = "COMMENT ON COLUMN ";
  private static final String TABLE_COMMENT = "COMMENT ON TABLE ";

  private static final String POSTGRESQL_NOT_SUPPORT_NESTED_COLUMN_MSG =
      "PostgreSQL does not support nested column names.";

  private String database;
  private PostgreSqlSchemaOperations schemaOperations;

  @Override
  public void initialize(
      DataSource dataSource,
      JdbcExceptionConverter exceptionMapper,
      JdbcTypeConverter jdbcTypeConverter,
      JdbcColumnDefaultValueConverter jdbcColumnDefaultValueConverter,
      Map<String, String> conf) {
    super.initialize(
        dataSource, exceptionMapper, jdbcTypeConverter, jdbcColumnDefaultValueConverter, conf);
    database = new JdbcConfig(conf).getJdbcDatabase();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(database),
        "The `jdbc-database` configuration item is mandatory in PostgreSQL.");
  }

  @Override
  public void setDatabaseOperation(DatabaseOperation databaseOperation) {
    this.schemaOperations = (PostgreSqlSchemaOperations) databaseOperation;
  }

  @Override
  public List<String> listTables(String schemaName) throws NoSuchSchemaException {
    try (Connection connection = getConnection(schemaName)) {
      if (!schemaOperations.schemaExists(connection, schemaName)) {
        throw new NoSuchSchemaException("No such schema: %s", schemaName);
      }
      final List<String> names = Lists.newArrayList();
      try (ResultSet tables = getTables(connection)) {
        while (tables.next()) {
          if (Objects.equals(tables.getString("TABLE_SCHEM"), schemaName)) {
            names.add(tables.getString("TABLE_NAME"));
          }
        }
      }
      LOG.info("Finished listing tables size {} for database name {} ", names.size(), schemaName);
      return names;
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  protected JdbcTable.Builder getTableBuilder(
      ResultSet tablesResult, String databaseName, String tableName) throws SQLException {
    boolean found = false;
    JdbcTable.Builder builder = null;
    while (tablesResult.next() && !found) {
      String tableNameInResult = tablesResult.getString("TABLE_NAME");
      String tableSchemaInResultLowerCase = tablesResult.getString("TABLE_SCHEM");
      if (Objects.equals(tableNameInResult, tableName)
          && Objects.equals(tableSchemaInResultLowerCase, databaseName)) {
        builder = getBasicJdbcTableInfo(tablesResult);
        found = true;
      }
    }

    if (!found) {
      throw new NoSuchTableException("Table %s does not exist in %s.", tableName, databaseName);
    }

    return builder;
  }

  @Override
  protected JdbcColumn.Builder getColumnBuilder(
      ResultSet columnsResult, String databaseName, String tableName) throws SQLException {
    JdbcColumn.Builder builder = null;
    if (Objects.equals(columnsResult.getString("TABLE_NAME"), tableName)
        && Objects.equals(columnsResult.getString("TABLE_SCHEM"), databaseName)) {
      builder = getBasicJdbcColumnInfo(columnsResult);
    }
    return builder;
  }

  @Override
  protected String generateCreateTableSql(
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      Index[] indexes) {
    if (ArrayUtils.isNotEmpty(partitioning)) {
      throw new UnsupportedOperationException(
          "Currently we do not support Partitioning in PostgreSQL");
    }
    Preconditions.checkArgument(
        Distributions.NONE.equals(distribution), "PostgreSQL does not support distribution");

    SqlBuilder sql = new SqlBuilder(PG_QUOTE);
    sql.append("CREATE TABLE ").identifier(tableName).append(" (").append(NEW_LINE);

    // Add columns
    for (int i = 0; i < columns.length; i++) {
      JdbcColumn column = columns[i];
      sql.append("    ").identifier(column.name());

      appendColumnDefinition(column, sql);
      // Add a comma for the next column, unless it's the last one
      if (i < columns.length - 1) {
        sql.append(",").append(NEW_LINE);
      }
    }
    appendIndexesSql(indexes, sql);
    sql.append(NEW_LINE).append(")");
    // Add table properties if any
    if (MapUtils.isNotEmpty(properties)) {
      // TODO #804 will add properties
      throw new IllegalArgumentException("Properties are not supported yet");
    }

    sql.append(";");

    // Add table comment if specified
    if (StringUtils.isNotEmpty(comment)) {
      sql.append(NEW_LINE)
          .append(TABLE_COMMENT)
          .identifier(tableName)
          .append(IS)
          .literal(comment)
          .append("';");
    }
    Arrays.stream(columns)
        .filter(jdbcColumn -> StringUtils.isNotEmpty(jdbcColumn.comment()))
        .forEach(
            jdbcColumn ->
                sql.append(NEW_LINE)
                    .append(COLUMN_COMMENT)
                    .identifier(tableName)
                    .append(".")
                    .identifier(jdbcColumn.name())
                    .append(IS)
                    .literal(jdbcColumn.comment())
                    .append("';"));

    // Return the generated SQL statement
    String result = sql.build();

    LOG.info("Generated create table:{} sql: {}", tableName, result);
    return result;
  }

  @VisibleForTesting
  static void appendIndexesSql(Index[] indexes, SqlBuilder sql) {
    for (Index index : indexes) {
      String fieldStr = getIndexFieldStr(index.fieldNames(), sql);
      sql.append(",").append(NEW_LINE);
      switch (index.type()) {
        case PRIMARY_KEY:
          if (StringUtils.isNotEmpty(index.name())) {
            sql.append("CONSTRAINT ").identifier(index.name());
          }
          sql.append(" PRIMARY KEY (").append(fieldStr).append(")");
          break;
        case UNIQUE_KEY:
          if (StringUtils.isNotEmpty(index.name())) {
            sql.append("CONSTRAINT ").identifier(index.name());
          }
          sql.append(" UNIQUE (").append(fieldStr).append(")");
          break;
        default:
          throw new IllegalArgumentException("PostgreSQL doesn't support index : " + index.type());
      }
    }
  }

  protected static String getIndexFieldStr(String[][] fieldNames, SqlBuilder sql) {
    return Arrays.stream(fieldNames)
        .map(
            colNames -> {
              if (colNames.length > 1) {
                throw new IllegalArgumentException(
                    "Index does not support complex fields in PostgreSQL");
              }
              return sql.quoteIdentifier(colNames[0]);
            })
        .collect(Collectors.joining(", "));
  }

  private void appendColumnDefinition(JdbcColumn column, SqlBuilder sql) {
    // Add data type
    sql.append(SPACE).append(typeConverter.fromGravitino(column.dataType())).append(SPACE);

    if (column.autoIncrement()) {
      if (!Types.allowAutoIncrement(column.dataType())) {
        throw new IllegalArgumentException(
            "Unsupported auto-increment , column: "
                + column.name()
                + ", type: "
                + column.dataType());
      }
      sql.append("GENERATED BY DEFAULT AS IDENTITY ");
    }

    // Add NOT NULL if the column is marked as such
    if (column.nullable()) {
      sql.append("NULL ");
    } else {
      sql.append("NOT NULL ");
    }
    // Add DEFAULT value if specified
    if (!DEFAULT_VALUE_NOT_SET.equals(column.defaultValue())) {
      sql.append("DEFAULT ")
          .append(columnDefaultValueConverter.fromGravitino(column.defaultValue()))
          .append(SPACE);
    }
  }

  @Override
  protected String generateRenameTableSql(String oldTableName, String newTableName) {
    SqlBuilder sql = new SqlBuilder(PG_QUOTE);
    return sql.append(ALTER_TABLE)
        .identifier(oldTableName)
        .append(" RENAME TO ")
        .identifier(newTableName)
        .build();
  }

  @Override
  protected String generateDropTableSql(String tableName) {
    SqlBuilder sql = new SqlBuilder(PG_QUOTE);
    return sql.append("DROP TABLE ").identifier(tableName).build();
  }

  @Override
  protected String generatePurgeTableSql(String tableName) {
    throw new UnsupportedOperationException(
        "PostgreSQL does not support purge table in Gravitino, please use drop table");
  }

  @Override
  protected String generateAlterTableSql(
      String schemaName, String tableName, TableChange... changes) {
    // Not all operations require the original table information, so lazy loading is used here
    JdbcTable lazyLoadTable = null;
    List<String> alterSql = new ArrayList<>();
    for (TableChange change : changes) {
      if (change instanceof TableChange.UpdateComment) {
        lazyLoadTable = getOrCreateTable(schemaName, tableName, lazyLoadTable);
        alterSql.add(updateCommentDefinition((TableChange.UpdateComment) change, lazyLoadTable));
      } else if (change instanceof TableChange.SetProperty) {
        throw new IllegalArgumentException("Set property is not supported yet");
      } else if (change instanceof TableChange.RemoveProperty) {
        // PostgreSQL does not support deleting table attributes, it can be replaced by Set Property
        throw new IllegalArgumentException("Remove property is not supported yet");
      } else if (change instanceof TableChange.AddColumn) {
        TableChange.AddColumn addColumn = (TableChange.AddColumn) change;
        lazyLoadTable = getOrCreateTable(schemaName, tableName, lazyLoadTable);
        alterSql.addAll(addColumnFieldDefinition(addColumn, lazyLoadTable));
      } else if (change instanceof TableChange.RenameColumn) {
        TableChange.RenameColumn renameColumn = (TableChange.RenameColumn) change;
        alterSql.add(renameColumnFieldDefinition(renameColumn, tableName));
      } else if (change instanceof TableChange.UpdateColumnDefaultValue) {
        lazyLoadTable = getOrCreateTable(schemaName, tableName, lazyLoadTable);
        TableChange.UpdateColumnDefaultValue updateColumnDefaultValue =
            (TableChange.UpdateColumnDefaultValue) change;
        alterSql.add(
            updateColumnDefaultValueFieldDefinition(updateColumnDefaultValue, lazyLoadTable));
      } else if (change instanceof TableChange.UpdateColumnType) {
        lazyLoadTable = getOrCreateTable(schemaName, tableName, lazyLoadTable);
        TableChange.UpdateColumnType updateColumnType = (TableChange.UpdateColumnType) change;
        alterSql.add(updateColumnTypeFieldDefinition(updateColumnType, lazyLoadTable));
      } else if (change instanceof TableChange.UpdateColumnComment) {
        alterSql.add(
            updateColumnCommentFieldDefinition(
                (TableChange.UpdateColumnComment) change, tableName));
      } else if (change instanceof TableChange.UpdateColumnPosition) {
        throw new IllegalArgumentException("PostgreSQL does not support column position.");
      } else if (change instanceof TableChange.DeleteColumn) {
        lazyLoadTable = getOrCreateTable(schemaName, tableName, lazyLoadTable);
        TableChange.DeleteColumn deleteColumn = (TableChange.DeleteColumn) change;
        String deleteColSql = deleteColumnFieldDefinition(deleteColumn, lazyLoadTable);
        if (StringUtils.isNotEmpty(deleteColSql)) {
          alterSql.add(deleteColSql);
        }
      } else if (change instanceof TableChange.UpdateColumnNullability) {
        TableChange.UpdateColumnNullability updateColumnNullability =
            (TableChange.UpdateColumnNullability) change;

        lazyLoadTable = getOrCreateTable(schemaName, tableName, lazyLoadTable);
        validateUpdateColumnNullable(updateColumnNullability, lazyLoadTable);

        alterSql.add(updateColumnNullabilityDefinition(updateColumnNullability, tableName));
      } else if (change instanceof TableChange.AddIndex) {
        alterSql.add(addIndexDefinition(tableName, (TableChange.AddIndex) change));
      } else if (change instanceof TableChange.DeleteIndex) {
        alterSql.add(deleteIndexDefinition(tableName, (TableChange.DeleteIndex) change));
      } else if (change instanceof TableChange.UpdateColumnAutoIncrement) {
        alterSql.add(
            updateColumnAutoIncrementDefinition(
                (TableChange.UpdateColumnAutoIncrement) change, tableName));
      } else {
        throw new IllegalArgumentException(
            "Unsupported table change type: " + change.getClass().getName());
      }
    }

    // If there is no change, return directly
    if (alterSql.isEmpty()) {
      return "";
    }

    // Return the generated SQL statement
    String result = String.join("\n", alterSql);
    LOG.info("Generated alter table:{}.{} sql: {}", schemaName, tableName, result);
    return result;
  }

  @VisibleForTesting
  static String updateColumnAutoIncrementDefinition(
      TableChange.UpdateColumnAutoIncrement change, String tableName) {
    if (change.fieldName().length > 1) {
      throw new UnsupportedOperationException(POSTGRESQL_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    SqlBuilder sql = new SqlBuilder(PG_QUOTE);
    String fieldName = change.fieldName()[0];
    String action =
        change.isAutoIncrement() ? "ADD GENERATED BY DEFAULT AS IDENTITY" : "DROP IDENTITY";

    return sql.append("ALTER TABLE ")
        .identifier(tableName)
        .append(" ")
        .append(ALTER_COLUMN)
        .identifier(fieldName)
        .append(" ")
        .append(action)
        .append(";")
        .build();
  }

  @VisibleForTesting
  static String deleteIndexDefinition(String tableName, TableChange.DeleteIndex deleteIndex) {
    SqlBuilder sql = new SqlBuilder(PG_QUOTE);
    sql.append("ALTER TABLE ")
        .identifier(tableName)
        .append(" DROP CONSTRAINT ")
        .identifier(deleteIndex.getName())
        .append(";\n");
    if (deleteIndex.isIfExists()) {
      sql.append("DROP INDEX IF EXISTS ").identifier(deleteIndex.getName()).append(";");
    } else {
      sql.append("DROP INDEX ").identifier(deleteIndex.getName()).append(";");
    }
    return sql.build();
  }

  @VisibleForTesting
  static String addIndexDefinition(String tableName, TableChange.AddIndex addIndex) {
    SqlBuilder sql = new SqlBuilder(PG_QUOTE);
    sql.append("ALTER TABLE ")
        .identifier(tableName)
        .append(" ADD CONSTRAINT ")
        .identifier(addIndex.getName());
    switch (addIndex.getType()) {
      case PRIMARY_KEY:
        sql.append(" PRIMARY KEY ");
        break;
      case UNIQUE_KEY:
        sql.append(" UNIQUE ");
        break;
      default:
        throw new IllegalArgumentException("Unsupported index type: " + addIndex.getType());
    }
    sql.append("(").append(getIndexFieldStr(addIndex.getFieldNames(), sql)).append(");");
    return sql.build();
  }

  private String updateColumnNullabilityDefinition(
      TableChange.UpdateColumnNullability updateColumnNullability, String tableName) {
    if (updateColumnNullability.fieldName().length > 1) {
      throw new UnsupportedOperationException(POSTGRESQL_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    SqlBuilder sql = new SqlBuilder(PG_QUOTE);
    String col = updateColumnNullability.fieldName()[0];
    if (updateColumnNullability.nullable()) {
      return sql.append(ALTER_TABLE)
          .identifier(tableName)
          .append(" ")
          .append(ALTER_COLUMN)
          .identifier(col)
          .append(" DROP NOT NULL;")
          .build();
    } else {
      return sql.append(ALTER_TABLE)
          .identifier(tableName)
          .append(" ")
          .append(ALTER_COLUMN)
          .identifier(col)
          .append(" SET NOT NULL;")
          .build();
    }
  }

  private String updateCommentDefinition(
      TableChange.UpdateComment updateComment, JdbcTable jdbcTable) {
    String newComment = updateComment.getNewComment();
    if (null == StringIdentifier.fromComment(newComment)) {
      // Detect and add Gravitino id.
      if (StringUtils.isNotEmpty(jdbcTable.comment())) {
        StringIdentifier identifier = StringIdentifier.fromComment(jdbcTable.comment());
        if (null != identifier) {
          newComment = StringIdentifier.addToComment(identifier, newComment);
        }
      }
    }
    SqlBuilder sql = new SqlBuilder(PG_QUOTE);
    return sql.append(TABLE_COMMENT)
        .identifier(jdbcTable.name())
        .append(IS)
        .literal(newComment)
        .append("';")
        .build();
  }

  private String deleteColumnFieldDefinition(
      TableChange.DeleteColumn deleteColumn, JdbcTable table) {
    if (deleteColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException(POSTGRESQL_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String col = deleteColumn.fieldName()[0];
    boolean colExists =
        Arrays.stream(table.columns()).anyMatch(s -> StringUtils.equals(col, s.name()));
    if (!colExists) {
      if (BooleanUtils.isTrue(deleteColumn.getIfExists())) {
        return "";
      } else {
        throw new IllegalArgumentException("Delete column does not exist: " + col);
      }
    }
    SqlBuilder sql = new SqlBuilder(PG_QUOTE);
    return sql.append(ALTER_TABLE)
        .identifier(table.name())
        .append(" DROP COLUMN ")
        .identifier(deleteColumn.fieldName()[0])
        .append(";")
        .build();
  }

  private String updateColumnDefaultValueFieldDefinition(
      TableChange.UpdateColumnDefaultValue updateColumnDefaultValue, JdbcTable jdbcTable) {
    if (updateColumnDefaultValue.fieldName().length > 1) {
      throw new UnsupportedOperationException(POSTGRESQL_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String col = updateColumnDefaultValue.fieldName()[0];
    JdbcColumn column =
        (JdbcColumn)
            Arrays.stream(jdbcTable.columns())
                .filter(c -> c.name().equals(col))
                .findFirst()
                .orElse(null);
    if (null == column) {
      throw new NoSuchColumnException("Column %s does not exist.", col);
    }

    SqlBuilder sql = new SqlBuilder(PG_QUOTE);
    sql.append(ALTER_TABLE)
        .identifier(jdbcTable.name())
        .append("\n")
        .append(ALTER_COLUMN)
        .identifier(col)
        .append(" SET DEFAULT ")
        .append(
            columnDefaultValueConverter.fromGravitino(
                updateColumnDefaultValue.getNewDefaultValue()));
    return sql.append(";").build();
  }

  private String updateColumnTypeFieldDefinition(
      TableChange.UpdateColumnType updateColumnType, JdbcTable jdbcTable) {
    if (updateColumnType.fieldName().length > 1) {
      throw new UnsupportedOperationException(POSTGRESQL_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String col = updateColumnType.fieldName()[0];
    JdbcColumn column =
        (JdbcColumn)
            Arrays.stream(jdbcTable.columns())
                .filter(c -> c.name().equals(col))
                .findFirst()
                .orElse(null);
    if (null == column) {
      throw new NoSuchColumnException("Column %s does not exist.", col);
    }
    SqlBuilder sql = new SqlBuilder(PG_QUOTE);
    sql.append(ALTER_TABLE)
        .identifier(jdbcTable.name())
        .append("\n")
        .append(ALTER_COLUMN)
        .identifier(col)
        .append(" SET DATA TYPE ")
        .append(typeConverter.fromGravitino(updateColumnType.getNewDataType()));
    if (!column.nullable()) {
      sql.append(",\n").append(ALTER_COLUMN).identifier(col).append(" SET NOT NULL");
    }
    return sql.append(";").build();
  }

  private String renameColumnFieldDefinition(
      TableChange.RenameColumn renameColumn, String tableName) {
    if (renameColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException(POSTGRESQL_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    SqlBuilder sql = new SqlBuilder(PG_QUOTE);
    return sql.append(ALTER_TABLE)
        .identifier(tableName)
        .append(" RENAME COLUMN ")
        .identifier(renameColumn.fieldName()[0])
        .append(SPACE)
        .append("TO")
        .append(SPACE)
        .identifier(renameColumn.getNewName())
        .append(";")
        .build();
  }

  private List<String> addColumnFieldDefinition(
      TableChange.AddColumn addColumn, JdbcTable lazyLoadTable) {
    if (addColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException(POSTGRESQL_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    List<String> result = new ArrayList<>();
    String col = addColumn.fieldName()[0];

    SqlBuilder sql = new SqlBuilder(PG_QUOTE);
    sql.append(ALTER_TABLE)
        .identifier(lazyLoadTable.name())
        .append(SPACE)
        .append("ADD COLUMN ")
        .identifier(col)
        .append(SPACE)
        .append(typeConverter.fromGravitino(addColumn.getDataType()))
        .append(SPACE);

    if (addColumn.isAutoIncrement()) {
      if (!Types.allowAutoIncrement(addColumn.getDataType())) {
        throw new IllegalArgumentException(
            "Unsupported auto-increment , column: "
                + Arrays.toString(addColumn.getFieldName())
                + ", type: "
                + addColumn.getDataType());
      }
      sql.append("GENERATED BY DEFAULT AS IDENTITY ");
    }

    // Add NOT NULL if the column is marked as such
    if (!addColumn.isNullable()) {
      sql.append("NOT NULL ");
    }

    // Append default value if available
    if (!Column.DEFAULT_VALUE_NOT_SET.equals(addColumn.getDefaultValue())) {
      sql.append("DEFAULT ")
          .append(columnDefaultValueConverter.fromGravitino(addColumn.getDefaultValue()))
          .append(SPACE);
    }

    // Append position if available
    if (!(addColumn.getPosition() instanceof TableChange.Default)) {
      throw new IllegalArgumentException(
          "PostgreSQL does not support column position in Gravitino.");
    }
    result.add(sql.append(";").build());

    // Append comment if available
    if (StringUtils.isNotEmpty(addColumn.getComment())) {
      SqlBuilder commentSql = new SqlBuilder(PG_QUOTE);
      result.add(
          commentSql
              .append(COLUMN_COMMENT)
              .identifier(lazyLoadTable.name())
              .append(".")
              .identifier(col)
              .append(IS)
              .literal(addColumn.getComment())
              .append("';")
              .build());
    }
    return result;
  }

  private String updateColumnCommentFieldDefinition(
      TableChange.UpdateColumnComment updateColumnComment, String tableName) {
    String newComment = updateColumnComment.getNewComment();
    if (updateColumnComment.fieldName().length > 1) {
      throw new UnsupportedOperationException(POSTGRESQL_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String col = updateColumnComment.fieldName()[0];
    SqlBuilder sql = new SqlBuilder(PG_QUOTE);
    return sql.append(COLUMN_COMMENT)
        .identifier(tableName)
        .append(".")
        .identifier(col)
        .append(IS)
        .literal(newComment)
        .append("';")
        .build();
  }

  @Override
  protected ResultSet getIndexInfo(String schemaName, String tableName, DatabaseMetaData metaData)
      throws SQLException {
    return metaData.getIndexInfo(database, schemaName, tableName, false, false);
  }

  @Override
  protected ResultSet getPrimaryKeys(String schemaName, String tableName, DatabaseMetaData metaData)
      throws SQLException {
    return metaData.getPrimaryKeys(database, schemaName, tableName);
  }

  @Override
  protected Connection getConnection(String schema) throws SQLException {
    Connection connection = dataSource.getConnection();
    connection.setCatalog(database);
    connection.setSchema(schema);
    return connection;
  }

  @Override
  protected ResultSet getTable(Connection connection, String schema, String tableName)
      throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    return metaData.getTables(database, schema, tableName, null);
  }

  @Override
  protected ResultSet getColumns(Connection connection, String schema, String tableName)
      throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    return metaData.getColumns(database, schema, tableName, null);
  }

  @Override
  public Integer calculateDatetimePrecision(String typeName, int columnSize, int scale) {
    String upperTypeName = typeName.toUpperCase();
    switch (upperTypeName) {
      case "TIME":
      case "TIMETZ":
      case "TIMESTAMP":
      case "TIMESTAMPTZ":
        return Math.max(scale, 0);
      default:
        return null;
    }
  }
}
