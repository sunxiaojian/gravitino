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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSqlBuilder {

  @Test
  public void testAppendRawSql() {
    SqlBuilder builder = new SqlBuilder();
    builder.append("CREATE TABLE ");
    Assertions.assertEquals("CREATE TABLE ", builder.toString());
  }

  @Test
  public void testAppendIdentifier() {
    SqlBuilder builder = new SqlBuilder();
    builder.append("CREATE TABLE ").appendIdentifier("my_table");
    Assertions.assertEquals("CREATE TABLE `my_table`", builder.toString());
  }

  @Test
  public void testAppendIdentifierWithBacktick() {
    SqlBuilder builder = new SqlBuilder();
    builder.appendIdentifier("my`table");
    Assertions.assertEquals("`my``table`", builder.toString());
  }

  @Test
  public void testAppendStringValue() {
    SqlBuilder builder = new SqlBuilder();
    builder.appendStringValue("normal value");
    Assertions.assertEquals("'normal value'", builder.toString());
  }

  @Test
  public void testAppendStringValueWithSingleQuote() {
    SqlBuilder builder = new SqlBuilder();
    builder.appendStringValue("O'Brien");
    Assertions.assertEquals("'O''Brien'", builder.toString());
  }

  @Test
  public void testAppendStringValueWithBackslash() {
    SqlBuilder builder = new SqlBuilder();
    builder.appendStringValue("path\\to\\file");
    Assertions.assertEquals("'path\\\\to\\\\file'", builder.toString());
  }

  @Test
  public void testSqlInjectionPrevention() {
    SqlBuilder builder = new SqlBuilder();
    // Malicious input that would cause SQL injection without escaping
    String maliciousComment = "test'; DROP TABLE users; --";
    builder.append("COMMENT ").appendStringValue(maliciousComment);

    // Should be safely escaped
    Assertions.assertEquals("COMMENT 'test''; DROP TABLE users; --'", builder.toString());
    // The escaped version is safe - it's just a string literal, not executable SQL
  }

  @Test
  public void testAppendComment() {
    SqlBuilder builder = new SqlBuilder();
    builder.appendComment("This is a comment");
    Assertions.assertEquals("COMMENT 'This is a comment'", builder.toString());
  }

  @Test
  public void testAppendCommentWithQuote() {
    SqlBuilder builder = new SqlBuilder();
    builder.appendComment("John's table");
    Assertions.assertEquals("COMMENT 'John''s table'", builder.toString());
  }

  @Test
  public void testAppendCommentEmpty() {
    SqlBuilder builder = new SqlBuilder();
    builder.appendComment("");
    Assertions.assertEquals("", builder.toString());
  }

  @Test
  public void testAppendCommentNull() {
    SqlBuilder builder = new SqlBuilder();
    builder.appendComment(null);
    Assertions.assertEquals("", builder.toString());
  }

  @Test
  public void testAppendStringValueNull() {
    SqlBuilder builder = new SqlBuilder();
    builder.appendStringValue(null);
    Assertions.assertEquals("NULL", builder.toString());
  }

  @Test
  public void testComplexSqlConstruction() {
    SqlBuilder builder = new SqlBuilder();
    builder
        .append("CREATE TABLE ")
        .appendIdentifier("users")
        .append(" (")
        .appendIdentifier("id")
        .append(" INT PRIMARY KEY, ")
        .appendIdentifier("name")
        .append(" VARCHAR(100) ")
        .appendComment("User's full name")
        .append(") ");

    String expected =
        "CREATE TABLE `users` (`id` INT PRIMARY KEY, `name` VARCHAR(100) COMMENT 'User''s full name') ";
    Assertions.assertEquals(expected, builder.toString());
  }

  @Test
  public void testClear() {
    SqlBuilder builder = new SqlBuilder();
    builder.append("Some SQL");
    Assertions.assertTrue(builder.length() > 0);

    builder.clear();
    Assertions.assertEquals(0, builder.length());
    Assertions.assertEquals("", builder.toString());
  }

  @Test
  public void testPostgreSqlIdentifierQuote() {
    // PostgreSQL uses double quotes for identifiers
    SqlBuilder builder = new SqlBuilder("\"");
    builder.appendIdentifier("my_table");
    Assertions.assertEquals("\"my_table\"", builder.toString());
  }

  @Test
  public void testMultipleSingleQuotes() {
    SqlBuilder builder = new SqlBuilder();
    builder.appendStringValue("It's a test's value");
    Assertions.assertEquals("'It''s a test''s value'", builder.toString());
  }

  @Test
  public void testSqlInjectionWithBackslashAndQuote() {
    SqlBuilder builder = new SqlBuilder();
    // Complex injection attempt
    String maliciousInput = "\\'; DROP TABLE users; --";
    builder.appendStringValue(maliciousInput);

    // Should be safely escaped: both backslash and quote
    Assertions.assertEquals("'\\\\''; DROP TABLE users; --'", builder.toString());
  }

  @Test
  public void testSqlInjectionInTableComment() {
    SqlBuilder builder = new SqlBuilder();
    // Simulate table comment with SQL injection attempt
    String maliciousComment = "Normal comment'; DROP TABLE users; --";
    builder.append("CREATE TABLE test (id INT) ").appendComment(maliciousComment);

    String expected = "CREATE TABLE test (id INT) COMMENT 'Normal comment''; DROP TABLE users; --'";
    Assertions.assertEquals(expected, builder.toString());

    // Verify the malicious SQL is safely escaped and cannot execute
    // Check that single quote is doubled (escaped)
    Assertions.assertTrue(builder.toString().contains("comment''"));
    // Verify it's wrapped in quotes
    Assertions.assertTrue(
        builder.toString().contains("COMMENT 'Normal comment''; DROP TABLE users; --'"));
  }

  @Test
  public void testSqlInjectionInColumnComment() {
    SqlBuilder builder = new SqlBuilder();
    // Simulate column comment with SQL injection
    String columnComment = "User ID'; DELETE FROM audit_log WHERE '1'='1";
    builder.append("id INT ").appendComment(columnComment);

    String expected = "id INT COMMENT 'User ID''; DELETE FROM audit_log WHERE ''1''=''1'";
    Assertions.assertEquals(expected, builder.toString());
  }

  @Test
  public void testMultipleSqlInjectionAttempts() {
    // Test various SQL injection patterns
    String[] injectionAttempts = {
      "'; DROP TABLE users; --",
      "' OR '1'='1",
      "'; TRUNCATE TABLE logs; --",
      "' UNION SELECT * FROM passwords; --",
      "'; INSERT INTO admin VALUES ('hacker'); --",
      "' AND SLEEP(10); --"
    };

    for (String injection : injectionAttempts) {
      SqlBuilder testBuilder = new SqlBuilder();
      testBuilder.appendStringValue(injection);
      String result = testBuilder.toString();

      // All should be wrapped in quotes and properly escaped
      Assertions.assertTrue(result.startsWith("'"), "Should start with quote");
      Assertions.assertTrue(result.endsWith("'"), "Should end with quote");
      // Verify the dangerous quotes are escaped (doubled)
      // Count single quotes: should have at least 2 at start/end + doubled quotes inside
      int singleQuoteCount = result.length() - result.replace("'", "").length();
      Assertions.assertTrue(
          singleQuoteCount >= 3, "Should have escaped quotes (at least start + doubled + end)");
    }
  }

  @Test
  public void testXssStyleInjectionInComment() {
    SqlBuilder builder = new SqlBuilder();
    // Test XSS-style injection (though not directly applicable to SQL)
    String xssAttempt = "<script>alert('xss')</script>'; DROP TABLE users; --";
    builder.appendComment(xssAttempt);

    String result = builder.toString();
    // Should be safely escaped
    Assertions.assertTrue(result.startsWith("COMMENT '"));
    Assertions.assertTrue(result.endsWith("'"));
    Assertions.assertTrue(result.contains("''; DROP TABLE users; --"));
  }

  @Test
  public void testUnicodeInjectionAttempt() {
    SqlBuilder builder = new SqlBuilder();
    // Test Unicode characters with injection
    String unicodeInjection = "测试表'; DELETE FROM 用户; --";
    builder.appendComment(unicodeInjection);

    String result = builder.toString();
    Assertions.assertEquals("COMMENT '测试表''; DELETE FROM 用户; --'", result);
  }

  @Test
  public void testNewlineInjectionAttempt() {
    SqlBuilder builder = new SqlBuilder();
    // Test injection with newlines
    String newlineInjection = "Line 1'\nDROP TABLE users;\n--";
    builder.appendStringValue(newlineInjection);

    String result = builder.toString();
    // Should escape the quote, newlines stay as-is (they're not special in SQL string literals)
    Assertions.assertTrue(result.contains("''"));
    Assertions.assertTrue(result.startsWith("'"));
    Assertions.assertTrue(result.endsWith("'"));
  }

  @Test
  public void testEmptyAndNullSafety() {
    SqlBuilder builder = new SqlBuilder();

    // Test empty comment
    builder.appendComment("");
    Assertions.assertEquals("", builder.toString());

    // Test null comment
    builder.clear();
    builder.appendComment(null);
    Assertions.assertEquals("", builder.toString());

    // Test null value
    builder.clear();
    builder.appendStringValue(null);
    Assertions.assertEquals("NULL", builder.toString());
  }
}
