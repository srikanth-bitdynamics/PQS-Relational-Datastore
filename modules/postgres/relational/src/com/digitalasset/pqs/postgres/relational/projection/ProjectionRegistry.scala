package com.digitalasset.pqs.postgres.relational.projection

import ujson.Value
import zio.ZIO
import zio.jdbc.*

object ProjectionRegistry:
  final case class Row(
      version: Long,
      status: String,
      hash: String,
      backfilledThroughIx: Option[Long],
      definition: Value
  )

  private def toRow(t: (Long, String, String, Option[Long], String)): Row =
    Row(t._1, t._2, t._3, t._4, ujson.read(t._5))

  def insertDraft(
      definition: Value,
      hash: String,
      resolvedShape: Value,
      layout: Int
  ): ZIO[ZConnection, Throwable, Long] =
    for
      next <- sql"select coalesce(max(projection_version), 0) + 1 from __query_projection"
        .query[Long]
        .selectOne
        .map(_.getOrElse(1L))
      _ <- sql"""insert into __query_projection
                   (projection_version, definition, definition_hash, resolved_shape, layout, status, created_at)
                 values ($next, ${ujson.write(definition)}::jsonb, $hash, ${ujson.write(resolvedShape)}::jsonb,
                         $layout, 'draft'::rel_projection_status, now())""".update
    yield next

  def list: ZIO[ZConnection, Throwable, Seq[Row]] =
    sql"""select projection_version, status::text, definition_hash, backfilled_through_ix, definition::text
          from __query_projection order by projection_version"""
      .query[(Long, String, String, Option[Long], String)]
      .selectAll
      .map(_.map(toRow).toSeq)

  def get(version: Long): ZIO[ZConnection, Throwable, Option[Row]] =
    sql"""select projection_version, status::text, definition_hash, backfilled_through_ix, definition::text
          from __query_projection where projection_version = $version"""
      .query[(Long, String, String, Option[Long], String)]
      .selectOne
      .map(_.map(toRow))

  def getByHash(hash: String): ZIO[ZConnection, Throwable, Option[Row]] =
    sql"""select projection_version, status::text, definition_hash, backfilled_through_ix, definition::text
          from __query_projection where definition_hash = $hash"""
      .query[(Long, String, String, Option[Long], String)]
      .selectOne
      .map(_.map(toRow))
end ProjectionRegistry
