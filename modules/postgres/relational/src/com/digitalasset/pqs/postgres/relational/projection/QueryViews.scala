// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.relational.projection

import zio.ZIO
import zio.jdbc.*

object QueryViews:
  private val baseColumns = Seq(
    "contract_id",
    "representative_package_id",
    "creation_package_id",
    "created_tx_ix",
    "created_at_offset",
    "creation_synchronizer_id",
    "signatories",
    "observers"
  )

  def publish(active: Map[String, Shape.ResolvedShape]): ZIO[ZConnection, Throwable, Unit] =
    sql"""select package_name, module_name, entity_name, base_table, query_view
          from __rel_entity where kind = 'template' and query_view is not null"""
      .query[(String, String, String, String, String)]
      .selectAll
      .flatMap(entities =>
        ZIO.foreachDiscard(entities) { (pkg, module, entity, base, view) =>
          for
            existing <- sql"""select a.attname from pg_attribute a
                              join pg_class c on c.oid = a.attrelid
                              join pg_namespace n on n.oid = c.relnamespace
                              where n.nspname = current_schema() and c.relname = $view
                                and a.attnum > 0 and not a.attisdropped order by a.attnum"""
              .query[String]
              .selectAll
            _ <- active.get(s"$pkg:$module:$entity") match
              case Some(shape)              => create(view, base, shape, existing.toSeq)
              case None if existing.isEmpty => ZIO.unit
              case None                     => incompatible(view, existing.toSeq)
          yield ()
        }
      )

  private def create(
      view: String,
      base: String,
      shape: Shape.ResolvedShape,
      existing: Seq[String]
  ): ZIO[ZConnection, Throwable, Unit] =
    val columns = baseColumns ++ shape.promoted.map(_.name) :+ "payload_json"
    val removed = existing.filterNot(columns.contains)
    if removed.nonEmpty then incompatible(view, removed)
    else
      val ordered = existing ++ columns.filterNot(existing.contains)
      val select = ordered
        .map {
          case "creation_package_id" =>
            val creation = quoteIdent("creation_package_id")
            SqlFragment(s"coalesce(c.$creation, c.${quoteIdent("representative_package_id")}) as $creation")
          case name =>
            SqlFragment(s"${if baseColumns.contains(name) then "c" else "p"}.${quoteIdent(name)}")
        }
        .mkFragment(sql", ")
      (SqlFragment(s"create or replace view ${quoteIdent(view)} as select ") ++ select ++
        SqlFragment(s""" from ${quoteIdent(base)} p
                       join __rel_contracts c on c.contract_pk = p.contract_pk
                       where c.created_tx_ix <= (select latest_ix())
                         and c.life_ix @> (select latest_ix())
                         and c.redaction_id is null
                         and not c.divulged_only""")).execute.unit

  private def incompatible(view: String, removed: Seq[String]): ZIO[Any, Throwable, Nothing] =
    ZIO.fail(
      new IllegalArgumentException(
        s"cannot republish $view: removing published columns (${removed.mkString(", ")}) requires an explicit consumer migration"
      )
    )

  private def quoteIdent(name: String): String = "\"" + name.replace("\"", "\"\"") + "\""
end QueryViews
