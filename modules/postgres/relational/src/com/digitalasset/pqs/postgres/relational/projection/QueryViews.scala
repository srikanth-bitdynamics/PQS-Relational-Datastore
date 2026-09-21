package com.digitalasset.pqs.postgres.relational.projection

import zio.ZIO
import zio.jdbc.*

object QueryViews:
  private val baseColumns: Seq[SqlFragment] = Seq(
    "c.contract_id",
    "c.representative_package_id",
    "c.creation_package_id",
    "c.created_tx_ix",
    "c.created_at_offset",
    "c.creation_synchronizer_id",
    "c.signatories",
    "c.observers"
  ).map(SqlFragment(_))

  def publish(active: Map[String, Shape.ResolvedShape]): ZIO[ZConnection, Throwable, Unit] =
    sql"""select package_name, module_name, entity_name, base_table, query_view
          from __rel_entity where kind = 'template' and query_view is not null"""
      .query[(String, String, String, String, String)]
      .selectAll
      .flatMap(entities =>
        ZIO.foreachDiscard(entities) { (pkg, module, entity, base, view) =>
          active.get(s"$pkg:$module:$entity") match
            case Some(shape) => drop(view) *> create(view, base, shape)
            case None        => drop(view)
        }
      )

  private def create(view: String, base: String, shape: Shape.ResolvedShape): ZIO[ZConnection, Throwable, Unit] =
    val typed  = shape.promoted.map(f => SqlFragment(s"p.${quoteIdent(f.name)}"))
    val select = (baseColumns ++ typed :+ SqlFragment("p.payload_json")).mkFragment(sql", ")
    (SqlFragment(s"create view ${view} as select ") ++ select ++
      SqlFragment(s""" from ${base} p
                       join __rel_contracts c on c.contract_pk = p.contract_pk
                       where c.created_tx_ix <= latest_ix()
                         and c.life_ix @> latest_ix()
                         and c.redaction_id is null
                         and not c.divulged_only""")).execute.unit

  private def drop(view: String): ZIO[ZConnection, Throwable, Unit] =
    SqlFragment(s"drop view if exists ${view}").execute.unit

  private def quoteIdent(name: String): String = "\"" + name.replace("\"", "\"\"") + "\""
end QueryViews
