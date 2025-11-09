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
package org.apache.gravitino.catalog.jdbc.operation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.utils.SqlBuilder;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;

public class SqliteTableOperations extends JdbcTableOperations {

  private static final String SQLITE_QUOTE = "\"";

  @Override
  protected String generateCreateTableSql(
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      Index[] indexes) {
    Preconditions.checkArgument(
        distribution == Distributions.NONE, "SQLite does not support distribution");

    SqlBuilder sql = new SqlBuilder(SQLITE_QUOTE);
    sql.append("CREATE TABLE ").identifier(tableName).append(" (");

    for (int i = 0; i < columns.length; i++) {
      JdbcColumn column = columns[i];
      sql.identifier(column.name())
          .append(" ")
          .append(typeConverter.fromGravitino(column.dataType()));
      if (!column.nullable()) {
        sql.append(" NOT NULL");
      }

      if (!Column.DEFAULT_VALUE_NOT_SET.equals(column.defaultValue())) {
        sql.append(" DEFAULT ");
        sql.append(columnDefaultValueConverter.fromGravitino(column.defaultValue()));
      }

      if (i < columns.length - 1) {
        sql.append(",");
      }
    }

    sql.append(")");
    if (comment != null && !comment.isEmpty()) {
      sql.append(" COMMENT ").literal(comment);
    }

    if (properties != null && !properties.isEmpty()) {
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        sql.append(" ").append(entry.getKey()).append("=").append(entry.getValue());
      }
    }
    sql.append(";");
    return sql.build();
  }

  @Override
  protected String generateRenameTableSql(String oldTableName, String newTableName) {
    SqlBuilder sql = new SqlBuilder(SQLITE_QUOTE);
    return sql.append("ALTER TABLE ")
        .identifier(oldTableName)
        .append(" RENAME TO ")
        .identifier(newTableName)
        .append(";")
        .build();
  }

  @Override
  protected String generateDropTableSql(String tableName) {
    SqlBuilder sql = new SqlBuilder(SQLITE_QUOTE);
    return sql.append("DROP TABLE ").identifier(tableName).append(";").build();
  }

  @Override
  protected String generatePurgeTableSql(String tableName) {
    throw new UnsupportedOperationException("Purge table is not supported in sqlite.");
  }

  @Override
  protected String generateAlterTableSql(
      String databaseName, String tableName, TableChange... changes) {
    throw new UnsupportedOperationException("Alter table is not supported in sqlite.");
  }

  @Override
  protected JdbcTable getOrCreateTable(
      String databaseName, String tableName, JdbcTable lazyLoadCreateTable) {
    throw new UnsupportedOperationException("Sqlite does not support lazy load create table.");
  }

  @Override
  protected boolean getAutoIncrementInfo(ResultSet columns) {
    return false;
  }

  @Override
  public List<String> listTables(String databaseName) throws NoSuchSchemaException {
    try (Connection connection = getConnection(databaseName)) {
      final List<String> names = Lists.newArrayList();
      try (ResultSet tables = getTables(connection)) {
        // tables.getString("TABLE_SCHEM") is always null.
        while (tables.next()) {
          names.add(tables.getString("TABLE_NAME"));
        }
      }
      LOG.info("Finished listing tables size {} for database name {} ", names.size(), databaseName);
      return names;
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }
}
