// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.relational.projection

import com.digitalasset.pqs.postgres.relational.WriterFence
import ujson.Value
import zio.ZIO
import zio.jdbc.*

object ProjectionRegistry:
  val projectionLockKey: Long = 0x70716a5f70726f6aL
  val writerLockKey: Long     = 0x70716a5f7772746cL

  final case class Row(
      version: Long,
      status: String,
      hash: String,
      shapeHash: String,
      backfilledThroughIx: Option[Long],
      definition: Value
  )

  private def toRow(t: (Long, String, String, String, Option[Long], String)): Row =
    Row(t._1, t._2, t._3, t._4, t._5, ujson.read(t._6))

  def insertDraft(
      definition: Value,
      hash: String,
      shapeHash: String,
      resolvedShape: Value,
      layout: Int
  ): ZIO[ZConnection, Throwable, Long] =
    for
      next <- sql"select coalesce(max(projection_version), 0) + 1 from __query_projection"
        .query[Long]
        .selectOne
        .map(_.getOrElse(1L))
      _ <- sql"""insert into __query_projection
                   (projection_version, definition, definition_hash, shape_hash, resolved_shape, layout,
                    status, backfilled_through_ix, created_at)
                 values ($next, ${ujson.write(definition)}::jsonb, $hash, $shapeHash,
                         ${ujson.write(resolvedShape)}::jsonb, $layout, 'draft'::rel_projection_status,
                         (select max(backfilled_through_ix) from __query_projection
                          where shape_hash = $shapeHash), now())""".update
    yield next

  def list: ZIO[ZConnection, Throwable, Seq[Row]] =
    sql"""select projection_version, status::text, definition_hash, shape_hash, backfilled_through_ix, definition::text
          from __query_projection order by projection_version"""
      .query[(Long, String, String, String, Option[Long], String)]
      .selectAll
      .map(_.map(toRow).toSeq)

  def get(version: Long): ZIO[ZConnection, Throwable, Option[Row]] =
    sql"""select projection_version, status::text, definition_hash, shape_hash, backfilled_through_ix, definition::text
          from __query_projection where projection_version = $version"""
      .query[(Long, String, String, String, Option[Long], String)]
      .selectOne
      .map(_.map(toRow))

  def getByHash(hash: String): ZIO[ZConnection, Throwable, Option[Row]] =
    sql"""select projection_version, status::text, definition_hash, shape_hash, backfilled_through_ix, definition::text
          from __query_projection where definition_hash = $hash and status <> 'retired'"""
      .query[(Long, String, String, String, Option[Long], String)]
      .selectOne
      .map(_.map(toRow))

  def latestDraft: ZIO[ZConnection, Throwable, Option[Long]] =
    sql"select max(projection_version) from __query_projection where status = 'draft'"
      .query[Option[Long]]
      .selectOne
      .map(_.flatten)

  def retireDrafts: ZIO[ZConnection, Throwable, Unit] =
    sql"update __query_projection set status = 'retired' where status = 'draft'".update.unit

  def resolvedShapeOf(version: Long): ZIO[ZConnection, Throwable, Option[String]] =
    sql"select resolved_shape::text from __query_projection where projection_version = $version"
      .query[String]
      .selectOne

  def setBackfilledThrough(version: Long, throughIx: Long): ZIO[ZConnection, Throwable, Unit] =
    sql"select 1 from pg_advisory_xact_lock($projectionLockKey)".query[Int].selectOne *>
      sql"""update __query_projection set backfilled_through_ix = $throughIx
             where projection_version = $version and status = 'draft'""".update
        .flatMap(_.compareTo(1L) match
          case 0 => ZIO.unit
          case _ => ZIO.fail(new IllegalStateException(s"projection $version is no longer a pending draft"))
        )

  def activate(version: Long): ZIO[ZConnection, Throwable, Unit] =
    for
      _ <- sql"select 1 from pg_advisory_xact_lock(${projectionLockKey})".query[Int].selectOne
      acquired <- sql"select case when pg_try_advisory_xact_lock(${writerLockKey}) then 1 else 0 end"
        .query[Int]
        .selectOne
        .map(_.getOrElse(0))
      _ <- acquired.compareTo(1) match
        case 0 => ZIO.unit
        case _ =>
          ZIO.fail(
            new RuntimeException(
              "cannot activate projection: a relational ingest writer is live; stop the writer, then re-run activate"
            )
          )
      _ <- WriterFence.drain
      _ <- ensureBackfilled(version)
      _ <- sql"update __query_projection set status = 'retired' where status = 'active'".update
      updated <- sql"""update __query_projection set status = 'active', activated_at = now()
                       where projection_version = $version and status = 'draft'""".update
      _ <- updated.compareTo(1L) match
        case 0 => ZIO.unit
        case _ =>
          ZIO.fail(
            new RuntimeException(
              s"cannot activate projection version $version: no draft version with that id is pending activation"
            )
          )
      shapeJson <- resolvedShapeOf(version)
      _         <- QueryViews.publish(shapeJson.fold(Map.empty)(ProjectionBinding.parse))
    yield ()

  private def ensureBackfilled(version: Long): ZIO[ZConnection, Throwable, Unit] =
    for
      row       <- get(version)
      watermark <- sql"select tx_ix from latest_checkpoint()".query[Long].selectOne.map(_.getOrElse(0L))
      _ <- row.flatMap(_.backfilledThroughIx) match
        case Some(through) if through >= watermark => ZIO.unit
        case Some(through) =>
          ZIO.fail(
            new RuntimeException(
              s"cannot activate projection version $version: backfilled through tx_ix $through but the watermark is $watermark; run backfill first"
            )
          )
        case None =>
          ZIO.fail(
            new RuntimeException(
              s"cannot activate projection version $version: not backfilled through the current watermark; run backfill first"
            )
          )
    yield ()
end ProjectionRegistry
