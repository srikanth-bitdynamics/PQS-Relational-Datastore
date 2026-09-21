package com.digitalasset.pqs.schema.postgres.relational

import zio.ZIO
import zio.jdbc.*

object RelationalQueries:
  private def exactlyOne[A](rows: Seq[A], what: String): ZIO[Any, Throwable, A] =
    rows match
      case Seq(one) => ZIO.succeed(one)
      case other    => ZIO.fail(new RuntimeException(s"expected exactly one $what but found ${other.size}: $other"))

  def lineageOf(entity: String): ZIO[ZConnection, Throwable, (String, String, String)] =
    sql"""select distinct e.package_name, e.module_name, e.entity_name
          from __rel_entity e join __rel_contracts c on c.template_entity_pk = e.pk
          where e.entity_name = $entity and e.kind = 'template'"""
      .query[(String, String, String)]
      .selectAll
      .flatMap(exactlyOne(_, s"$entity template lineage"))

  def baseTableOf(entity: String): ZIO[ZConnection, Throwable, String] =
    sql"""select distinct e.base_table
          from __rel_entity e join __rel_contracts c on c.template_entity_pk = e.pk
          where e.entity_name = $entity and e.kind = 'template'"""
      .query[String]
      .selectAll
      .flatMap(exactlyOne(_, s"$entity base table"))

  def queryViewOf(entity: String): ZIO[ZConnection, Throwable, String] =
    sql"""select distinct e.query_view
          from __rel_entity e join __rel_contracts c on c.template_entity_pk = e.pk
          where e.entity_name = $entity and e.kind = 'template'"""
      .query[String]
      .selectAll
      .flatMap(exactlyOne(_, s"$entity query view"))
end RelationalQueries
