/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.command

import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.analysis.AnalysisTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * This test suite covers the JSON output functionality for SHOW TBLPROPERTIES command.
 * It ensures that table properties can be displayed in JSON format.
 */
class ShowTblPropertiesJsonSuite extends QueryTest with SharedSparkSession with AnalysisTest {

  // Add implicit formats for json4s
  implicit val formats: Formats = DefaultFormats

  test("SHOW TBLPROPERTIES ... AS JSON - all properties") {
    withTable("t") {
      sql("CREATE TABLE t (id INT) USING parquet TBLPROPERTIES('key1'='value1', 'key2'='value2')")
      val result = sql("SHOW TBLPROPERTIES t AS JSON")
      val jsonStr = result.head().getString(0)
      val json = parse(jsonStr)

      // Verify the JSON structure
      assert(json \ "properties" != JNothing)
      val properties = (json \ "properties").asInstanceOf[JObject]

      // Check that our properties are present
      assert((properties \ "key1") == JString("value1"))
      assert((properties \ "key2") == JString("value2"))
    }
  }

  test("SHOW TBLPROPERTIES ... AS JSON - single property") {
    withTable("t") {
      sql("CREATE TABLE t (id INT) USING parquet TBLPROPERTIES('key1'='value1', 'key2'='value2')")
      val result = sql("SHOW TBLPROPERTIES t('key1') AS JSON")
      val jsonStr = result.head().getString(0)
      val json = parse(jsonStr)

      // Verify the JSON structure for single property
      assert((json \ "key") == JString("key1"))
      assert((json \ "value") == JString("value1"))
    }
  }

  test("SHOW TBLPROPERTIES ... AS JSON - non-existent property") {
    withTable("t") {
      sql("CREATE TABLE t (id INT) USING parquet TBLPROPERTIES('key1'='value1')")
      val result = sql("SHOW TBLPROPERTIES t('nonexistent') AS JSON")
      val jsonStr = result.head().getString(0)
      val json = parse(jsonStr)

      // Verify that the error message is in the JSON
      assert((json \ "key") == JString("nonexistent"))
      val valueStr = (json \ "value").extract[String]
      assert(valueStr.contains("does not have property"))
    }
  }

  test("SHOW TBLPROPERTIES ... AS JSON - view properties") {
    withView("v") {
      sql("CREATE VIEW v TBLPROPERTIES('viewKey'='viewValue') AS SELECT 1 AS id")
      val result = sql("SHOW TBLPROPERTIES v AS JSON")
      val jsonStr = result.head().getString(0)
      val json = parse(jsonStr)

      // Verify view properties are shown
      val properties = (json \ "properties").asInstanceOf[JObject]
      assert((properties \ "viewKey") == JString("viewValue"))
    }
  }

  test("SHOW TBLPROPERTIES ... AS JSON - temporary view") {
    withTempView("tv") {
      sql("CREATE TEMPORARY VIEW tv AS SELECT 1 AS id")
      val result = sql("SHOW TBLPROPERTIES tv AS JSON")
      val jsonStr = result.head().getString(0)

      // Temporary views should return empty JSON
      assert(jsonStr == "{}")
    }
  }

  test("SHOW TBLPROPERTIES ... AS JSON - verify output schema") {
    withTable("t") {
      sql("CREATE TABLE t (id INT) USING parquet TBLPROPERTIES('key1'='value1')")
      val result = sql("SHOW TBLPROPERTIES t AS JSON")

      // Verify schema
      assert(result.schema.fields.length == 1)
      assert(result.schema.fields(0).name == "json_metadata")
      assert(result.schema.fields(0).dataType.typeName == "string")
    }
  }

  test("SHOW TBLPROPERTIES ... AS JSON - properties are sorted alphabetically") {
    withTable("t") {
      sql("CREATE TABLE t (id INT) USING parquet " +
        "TBLPROPERTIES('zebra'='z', 'alpha'='a', 'middle'='m')")
      val result = sql("SHOW TBLPROPERTIES t AS JSON")
      val jsonStr = result.head().getString(0)
      val json = parse(jsonStr)

      val properties = (json \ "properties").asInstanceOf[JObject]
      val keys = properties.obj.map(_._1)

      // Verify keys are sorted
      assert(keys == keys.sorted)
      assert(keys.head == "alpha")
    }
  }

  test("SHOW TBLPROPERTIES ... AS JSON - sensitive data redaction") {
    withTable("t") {
      sql("CREATE TABLE t (id INT) USING parquet TBLPROPERTIES('password'='secret123')")
      val result = sql("SHOW TBLPROPERTIES t AS JSON")
      val jsonStr = result.head().getString(0)

      // Password should be redacted
      assert(jsonStr.contains("password"))
      assert(!jsonStr.contains("secret123"))
    }
  }
}
