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
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.SparkException
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{ResolvedPersistentView, ResolvedTable,
  ResolvedTempView}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ShowTableProperties}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, V1Table}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.types.{MetadataBuilder, StringType}

/**
 * The command for `SHOW TBLPROPERTIES ... AS JSON`.
 *
 * Unlike [[ShowTablesJsonCommand]] and [[ShowPartitionsJsonCommand]], `SHOW TBLPROPERTIES`
 * has four distinct property sources depending on what `child` resolves to, mirroring the
 * non-JSON command's own dispatch (spread across [[ShowTablePropertiesCommand]],
 * `ShowTablePropertiesExec`, and `ShowV2ViewPropertiesExec`):
 *
 *   - a temp view                                -> no properties (matches the text command)
 *   - a session-catalog persistent view or table  -> delegates to [[ShowTablePropertiesCommand]]
 *   - a non-session-catalog (v2) persistent view   -> reads `ResolvedPersistentView.info`
 *   - a non-session-catalog (v2) table             -> `CatalogV2Util.tablePropertiesForDisplay`
 *                                                     (stored + display-only properties)
 *
 * The catalog-type check here uses [[CatalogV2Util#isSessionCatalog]] for consistency with
 * [[ShowTablesJsonCommand]], which is a slightly coarser check than the private
 * `supportsV1Command` used internally by `ResolveSessionCatalog` (that method also special
 * cases `CatalogExtension`s). This is a known, narrow parity gap versus the text command for
 * that edge case.
 *
 * The output is a single row with a `json_metadata` column containing a JSON object of the
 * form: `{"properties": [{"key": "k", "value": "v"}, ...]}`. When a specific property key is
 * requested and it is not present, the array is empty rather than embedding a human-readable
 * "does not have property" sentence as a fake value -- unlike the text command, this output is
 * meant to be machine-parsed.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW TBLPROPERTIES multi_part_name[('propertyKey')] AS JSON;
 * }}}
 */
case class ShowTablePropertiesJsonCommand(
    child: LogicalPlan,
    propertyKey: Option[String],
    override val output: Seq[Attribute] = Seq(
      AttributeReference(
        "json_metadata",
        StringType,
        nullable = false,
        new MetadataBuilder()
          .putString("comment", "JSON metadata of the table properties")
          .build())()
    )) extends UnaryRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val properties = child match {
      case _: ResolvedTempView =>
        Map.empty[String, String]

      case ResolvedPersistentView(catalog, ident, _) if CatalogV2Util.isSessionCatalog(catalog) =>
        sessionCatalogProperties(
          sparkSession, ident.asTableIdentifier.copy(catalog = Some(catalog.name)))

      case ResolvedPersistentView(_, _, info) =>
        redactedProperties(sparkSession, info.properties)

      case ResolvedTable(catalog, _, t: V1Table, _) if CatalogV2Util.isSessionCatalog(catalog) =>
        sessionCatalogProperties(sparkSession, t.catalogTable.identifier)

      case ResolvedTable(_, _, table, _) =>
        CatalogV2Util.tablePropertiesForDisplay(
          table.properties(), table.displayProperties(), sparkSession.sessionState.conf)

      case other =>
        throw SparkException.internalError(
          s"Unexpected child in ShowTablePropertiesJsonCommand: ${other.getClass.getSimpleName}")
    }
    Seq(Row(compact(render(buildJson(properties)))))
  }

  /** Reuses [[ShowTablePropertiesCommand]] for the session-catalog table/view path so the
   * redaction, view-property filtering, and temp-view handling stay in exactly one place. */
  private def sessionCatalogProperties(
      sparkSession: SparkSession, ident: TableIdentifier): Map[String, String] = {
    ShowTablePropertiesCommand(ident, None, ShowTableProperties.getOutputAttrs)
      .run(sparkSession)
      .map(row => row.getString(0) -> row.getString(1))
      .toMap
  }

  /** Mirrors the reserved-key filtering `ShowV2ViewPropertiesExec` applies for v2 views. */
  private def redactedProperties(
      sparkSession: SparkSession,
      rawProperties: java.util.Map[String, String]): Map[String, String] = {
    import scala.jdk.CollectionConverters._
    sparkSession.sessionState.conf.redactOptions(rawProperties.asScala.toMap)
      .filter { case (k, _) => !CatalogV2Util.TABLE_RESERVED_PROPERTIES.contains(k) }
  }

  private def buildJson(properties: Map[String, String]): JObject = {
    val entries = propertyKey match {
      case Some(key) => properties.get(key).map(key -> _).toSeq
      case None => properties.toSeq.sortBy(_._1)
    }
    JObject("properties" -> JArray(entries.map { case (k, v) =>
      JObject("key" -> JString(k), "value" -> JString(v))
    }.toList))
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(child = newChild)
}
