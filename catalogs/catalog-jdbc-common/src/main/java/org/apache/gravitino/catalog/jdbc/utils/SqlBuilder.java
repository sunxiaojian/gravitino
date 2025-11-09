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
package org.apache.gravitino.catalog.jdbc.utils;

import org.apache.commons.lang3.StringUtils;

/**
 * SQL builder that prevents SQL injection by properly escaping identifiers and string literals.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * SqlBuilder sql = new SqlBuilder();
 * sql.append("CREATE TABLE ")
 *    .identifier(tableName)
 *    .append(" (")
 *    .identifier(columnName)
 *    .append(" VARCHAR(100) ")
 *    .comment(userComment)
 *    .append(")");
 * String result = sql.build();
 * }</pre>
 */
public class SqlBuilder {

  private final StringBuilder sql;
  private final String identifierQuoteString;
  private final String literalQuoteString;

  public SqlBuilder() {
    this("`", "'");
  }

  /**
   * Creates a SqlBuilder with custom identifier quoting.
   *
   * @param identifierQuoteString Quote character for identifiers
   */
  public SqlBuilder(String identifierQuoteString) {
    this(identifierQuoteString, "'");
  }

  /**
   * Creates a SqlBuilder with custom quoting characters.
   *
   * @param identifierQuoteString Quote character for identifiers
   * @param literalQuoteString Quote character for string literals
   */
  public SqlBuilder(String identifierQuoteString, String literalQuoteString) {
    this.sql = new StringBuilder();
    this.identifierQuoteString = identifierQuoteString;
    this.literalQuoteString = literalQuoteString;
  }

  /**
   * Appends raw SQL text without escaping. Use only for SQL keywords, never for user input.
   *
   * @param text SQL text to append
   * @return This builder
   */
  public SqlBuilder append(String text) {
    sql.append(text);
    return this;
  }

  /**
   * Quotes and appends an identifier using dialect-specific quoting rules.
   *
   * @param identifier Identifier to quote and append
   * @return This builder
   */
  public SqlBuilder appendIdentifier(String identifier) {
    if (identifier != null) {
      sql.append(quoteIdentifier(identifier));
    }
    return this;
  }

  /**
   * Shorthand for appendIdentifier().
   *
   * @param identifier Identifier to quote and append
   * @return This builder
   */
  public SqlBuilder identifier(String identifier) {
    return appendIdentifier(identifier);
  }

  /**
   * Quotes and appends a string literal with proper escaping.
   *
   * @param value String value to quote and append
   * @return This builder
   */
  public SqlBuilder appendStringValue(String value) {
    if (value == null) {
      sql.append("NULL");
    } else {
      sql.append(quoteStringLiteral(value));
    }
    return this;
  }

  /**
   * Shorthand for appendStringValue().
   *
   * @param value String value to quote and append
   * @return This builder
   */
  public SqlBuilder literal(String value) {
    return appendStringValue(value);
  }

  /**
   * Appends a COMMENT clause with properly quoted string literal.
   *
   * @param comment Comment text
   * @return This builder
   */
  public SqlBuilder appendComment(String comment) {
    if (StringUtils.isNotEmpty(comment)) {
      sql.append("COMMENT ").append(quoteStringLiteral(comment));
    }
    return this;
  }

  /**
   * Shorthand for appendComment().
   *
   * @param comment Comment text
   * @return This builder
   */
  public SqlBuilder comment(String comment) {
    return appendComment(comment);
  }

  /**
   * Quotes a string literal with proper escaping.
   *
   * @param value String value to quote
   * @return Quoted and escaped string literal
   */
  public String quoteStringLiteral(String value) {
    if (value == null) {
      return "NULL";
    }
    String escaped = value.replace("\\", "\\\\");
    escaped = escaped.replace(literalQuoteString, literalQuoteString + literalQuoteString);
    return literalQuoteString + escaped + literalQuoteString;
  }

  /**
   * Quotes an identifier according to SQL dialect.
   *
   * @param identifier Identifier to quote
   * @return Quoted identifier
   */
  public String quoteIdentifier(String identifier) {
    if (identifier == null) {
      return null;
    }
    String escaped =
        identifier.replace(identifierQuoteString, identifierQuoteString + identifierQuoteString);
    return identifierQuoteString + escaped + identifierQuoteString;
  }

  /**
   * Builds and returns the final SQL string.
   *
   * @return Constructed SQL string
   */
  public String build() {
    return sql.toString();
  }

  /**
   * Returns the current SQL string.
   *
   * @return Constructed SQL string
   */
  @Override
  public String toString() {
    return sql.toString();
  }

  /**
   * Returns the length of the SQL string.
   *
   * @return Length of SQL string
   */
  public int length() {
    return sql.length();
  }

  /**
   * Checks if the SQL builder is empty.
   *
   * @return true if empty
   */
  public boolean isEmpty() {
    return sql.length() == 0;
  }

  /**
   * Clears the SQL builder content.
   *
   * @return This builder
   */
  public SqlBuilder clear() {
    sql.setLength(0);
    return this;
  }
}
