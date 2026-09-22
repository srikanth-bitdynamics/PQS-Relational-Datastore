// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.schema.postgres.relational

import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.postgres.backend.transact
import com.digitalasset.pqs.postgres.relational.projection.{
  IndexManager,
  ProjectionDefinition,
  ProjectionQuery,
  ProjectionRegistry
}
import com.digitalasset.pqs.services.postgres.{Postgres, ProductionPool}
import zio.{Schedule, ZIO, durationInt}
import zio.jdbc.*
import zio.test.*

object IndexLifecycleSpec extends FuncTest[Postgres]:
  def shared = Postgres.instance

  private val qualified = "Test:Asset:Asset"
  private val definitions = Map(
    "asset" -> ProjectionDefinition(
      Seq(qualified),
      Seq("owner", "amount"),
      Seq(ProjectionQuery(Seq("owner"), Seq("amount desc")))
    )
  )
  private val planned = IndexManager
    .plan(
      definitions,
      Map("asset"   -> Map(qualified -> Seq("owner", "amount"))),
      Map(qualified -> "payload")
    )
    .indexes
    .headOption
    .getOrElse(throw new IllegalStateException("Expected an owner/amount index plan"))

  private val shape = ujson.Obj(
    "asset" -> ujson.Obj(
      qualified -> ujson.Arr(
        ujson.Obj("name" -> "owner"),
        ujson.Obj("name" -> "amount")
      )
    )
  )

  private val setup = ProductionPool.relationalSchema *> transact {
    for
      _ <- sql"create table payload (contract_pk bigint primary key, owner text, amount bigint)".execute
      _ <- sql"create table other_payload (contract_pk bigint primary key, owner text, amount bigint)".execute
      _ <- sql"""insert into __rel_entity (package_name, module_name, entity_name, kind, base_table)
                   values ('Test', 'Asset', 'Asset', 'template', 'payload')""".update
      v <- ProjectionRegistry.insertDraft(ProjectionDefinition.toJson(definitions), "v1", "shape-asset", shape, 1)
      _ <- sql"update __query_projection set status = 'active' where projection_version = $v".update
    yield ()
  }

  private def ddl(text: String) = transact(SqlFragment(text).execute)
  private val oldIndex = ddl("create index old_coverage on payload (owner)") *> transact(
    sql"""insert into __rel_managed_index
            (projection_version, table_name, index_name, definition, columns, status, adopted, created_at, physical_oid)
            values (1, 'payload', 'old_coverage', 'old', array['owner'], 'active', true, now(), 'old_coverage'::regclass::oid)""".update
  )
  private def status(name: String) = transact(
    sql"select status::text from __rel_managed_index where index_name = $name".query[String].selectOne
  )

  def spec = suite("managed index lifecycle")(
    funcTest("adoption preserves old coverage until every physical replacement is present and valid") {
      Given:
        Postgres.database >+> ProductionPool.layer(maxConnections = 1)
      Then:
        for
          _       <- setup *> oldIndex
          _       <- IndexManager.adopt
          before  <- status("old_coverage")
          _       <- IndexManager.build *> IndexManager.adopt
          after   <- status("old_coverage")
          active  <- status(planned.name)
          _       <- IndexManager.retire
          retired <- status("old_coverage")
        yield assertTrue(
          before.contains("active"),
          after.contains("retiring"),
          active.contains("active"),
          retired.contains("retired")
        )
    },
    funcTest("an abandoned building index with no recorded identity is recovered and dropped") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _ <- setup *> ddl("create index abandoned_build on payload (owner)")
          _ <- transact(
            sql"""insert into __rel_managed_index
                    (projection_version, table_name, index_name, definition, columns, key_directions,
                     included_columns, status, adopted, created_at)
                  values (1, 'payload', 'abandoned_build', 'abandoned', array['owner'], array['0'],
                          array[]::text[], 'building'::rel_index_status, false, now())""".update
          )
          _        <- IndexManager.build *> IndexManager.adopt
          stranded <- status("abandoned_build")
          report   <- IndexManager.retire
          after    <- status("abandoned_build")
          exists   <- transact(sql"select to_regclass('abandoned_build') is not null".query[Boolean].selectOne)
        yield assertTrue(
          stranded.contains("retiring"),
          report.contains("Retired 1"),
          after.contains("retired"),
          exists.contains(false)
        )
    },
    funcTest("an unrecorded index whose shape differs from the registry is not dropped") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _ <- setup *> ddl("create index foreign_build on payload (amount)")
          _ <- transact(
            sql"""insert into __rel_managed_index
                    (projection_version, table_name, index_name, definition, columns, key_directions,
                     included_columns, status, adopted, created_at)
                  values (1, 'payload', 'foreign_build', 'foreign', array['owner'], array['0'],
                          array[]::text[], 'retiring'::rel_index_status, false, now())""".update
          )
          report <- IndexManager.retire
          after  <- status("foreign_build")
          exists <- transact(sql"select to_regclass('foreign_build') is not null".query[Boolean].selectOne)
        yield assertTrue(
          report.contains("does not match its registered definition"),
          after.contains("retiring"),
          exists.contains(true)
        )
    },
    funcTest("an unrecorded index whose key direction differs from the registry is not dropped") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _ <- setup *> ddl("create index direction_build on payload (owner, amount)")
          _ <- transact(
            sql"""insert into __rel_managed_index
                    (projection_version, table_name, index_name, definition, columns, key_directions,
                     included_columns, status, adopted, created_at)
                  values (1, 'payload', 'direction_build', 'direction', array['owner', 'amount'],
                          array['0', '3'], array[]::text[], 'retiring'::rel_index_status, false, now())""".update
          )
          report <- IndexManager.retire
          after  <- status("direction_build")
          exists <- transact(sql"select to_regclass('direction_build') is not null".query[Boolean].selectOne)
        yield assertTrue(
          report.contains("does not match its registered definition"),
          after.contains("retiring"),
          exists.contains(true)
        )
    },
    funcTest("an unmanaged same-name index is neither adopted nor deleted") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _      <- setup *> ddl(s"create index ${planned.name} on other_payload (amount)")
          result <- IndexManager.build.either
          _      <- IndexManager.adopt
          exists <- transact(sql"select to_regclass(${planned.name}) is not null".query[Boolean].selectOne)
          rows   <- transact(sql"select count(*) from __rel_managed_index".query[Long].selectOne)
        yield assertTrue(result.isLeft, exists.contains(true), rows.contains(0L))
    },
    funcTest("adoption rejects changed keys, directions, includes, predicates, table, uniqueness and opclasses") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _ <- setup *> oldIndex *> IndexManager.build
          mismatches <- ZIO.foreach(
            Seq(
              "on payload (amount) include (contract_pk)",
              "on payload (owner, amount) include (contract_pk)",
              "on payload (owner, amount desc)",
              "on payload (owner, amount desc) include (contract_pk) where amount > 0",
              "on other_payload (owner, amount desc) include (contract_pk)",
              "on payload (owner text_pattern_ops, amount desc) include (contract_pk)"
            )
          ) { suffix =>
            for
              _ <- ddl(s"drop index ${planned.name}") *> ddl(s"create index ${planned.name} $suffix")
              _ <- transact(
                sql"update __rel_managed_index set physical_oid = null where index_name = ${planned.name}".update
              )
              _   <- IndexManager.adopt
              old <- status("old_coverage")
            yield old.contains("active")
          }
          _ <- ddl(s"drop index ${planned.name}") *>
            ddl(s"create unique index ${planned.name} on payload (owner, amount desc) include (contract_pk)")
          _ <- transact(
            sql"update __rel_managed_index set physical_oid = null where index_name = ${planned.name}".update
          )
          _              <- IndexManager.adopt
          uniqueRejected <- status("old_coverage")
        yield assertTrue(mismatches.forall(identity), uniqueRejected.contains("active"))
    },
    funcTest("same-named indexes in another schema do not satisfy or confuse validation") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _ <- setup *> ddl("create schema elsewhere") *>
            ddl("create table elsewhere.payload (contract_pk bigint, owner text, amount bigint)") *>
            ddl(s"create index ${planned.name} on elsewhere.payload (owner, amount desc) include (contract_pk)")
          _      <- IndexManager.adopt
          before <- status(planned.name)
          _      <- IndexManager.build *> IndexManager.adopt
          after  <- status(planned.name)
        yield assertTrue(before.isEmpty, after.contains("active"))
    },
    funcTest("a failed DROP keeps the index retiring for a later retry") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _ <- setup *> IndexManager.build
          _ <- ddl("alter table other_payload add constraint protected_index unique (owner)")
          _ <- transact(
            sql"""insert into __rel_managed_index
            (table_name, index_name, definition, columns, status, adopted, created_at, physical_oid)
            values ('other_payload', 'protected_index', 'protected', array['owner'], 'retiring', true, now(), 'protected_index'::regclass::oid)""".update
          )
          report <- IndexManager.retire
          after  <- status("protected_index")
        yield assertTrue(report.contains("failed"), after.contains("retiring"))
    },
    funcTest("reused physical indexes follow the newly active projection version") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _ <- setup *> IndexManager.build *> IndexManager.adopt
          v <- transact {
            sql"update __query_projection set status = 'retired'".update *>
              ProjectionRegistry
                .insertDraft(ProjectionDefinition.toJson(definitions), "v2", "shape-asset", shape, 1)
                .tap(v => sql"update __query_projection set status = 'active' where projection_version = $v".update)
          }
          _ <- IndexManager.build *> IndexManager.adopt
          version <- transact(
            sql"select projection_version from __rel_managed_index where index_name = ${planned.name}"
              .query[Long]
              .selectOne
          )
        yield assertTrue(version.contains(v))
    },
    funcTest("retirement refuses an externally replaced object even when its name is unchanged") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _      <- setup *> oldIndex *> IndexManager.build *> IndexManager.adopt
          _      <- ddl("drop index old_coverage") *> ddl("create index old_coverage on payload (owner)")
          report <- IndexManager.retire
          after  <- status("old_coverage")
          exists <- transact(sql"select to_regclass('old_coverage') is not null".query[Boolean].selectOne)
        yield assertTrue(report.contains("failed"), after.contains("retiring"), exists.contains(true))
    },
    funcTest("build recreates a missing managed index and validates its new physical identity") {
      Given:
        Postgres.database >+> ProductionPool.layer(maxConnections = 1)
      Then:
        for
          _ <- setup *> IndexManager.build *> IndexManager.adopt
          before <- transact(
            sql"select physical_oid::bigint from __rel_managed_index where index_name = ${planned.name}"
              .query[Long]
              .selectOne
          )
          _ <- ddl(s"drop index ${planned.name}") *> IndexManager.build *> IndexManager.adopt
          after <- transact(
            sql"select physical_oid::bigint from __rel_managed_index where index_name = ${planned.name}"
              .query[Long]
              .selectOne
          )
        yield assertTrue(before.isDefined, after.isDefined, before != after)
    },
    funcTest("a re-planned retiring index can be built and adopted again") {
      Given:
        Postgres.database >+> ProductionPool.layer(maxConnections = 1)
      Then:
        for
          _     <- setup *> IndexManager.build *> IndexManager.adopt
          _     <- transact(sql"update __rel_managed_index set status = 'retiring'".update)
          _     <- IndexManager.build *> IndexManager.adopt
          after <- status(planned.name)
        yield assertTrue(after.contains("active"))
    },
    funcTest("retry recreates an invalid managed index and records its new physical identity") {
      Given:
        Postgres.database >+> ProductionPool.layer(maxConnections = 1)
      Then:
        for
          _      <- setup *> IndexManager.build
          before <- transact(sql"select physical_oid::bigint from __rel_managed_index".query[Long].selectOne)
          _ <- transact(sql"update pg_index set indisvalid = false where indexrelid = ${planned.name}::regclass".update)
          _ <- IndexManager.build *> IndexManager.adopt
          after <- transact(sql"select physical_oid::bigint from __rel_managed_index".query[Long].selectOne)
          state <- status(planned.name)
        yield assertTrue(before.isDefined, after.isDefined, before != after, state.contains("active"))
    },
    funcTest("retry never drops an invalid external replacement with the same index name") {
      Given:
        Postgres.database >+> ProductionPool.layer(maxConnections = 1)
      Then:
        for
          _ <- setup *> IndexManager.build
          _ <- ddl(s"drop index ${planned.name}") *> ddl(s"create index ${planned.name} on other_payload (amount)")
          _ <- transact(sql"update pg_index set indisvalid = false where indexrelid = ${planned.name}::regclass".update)
          result <- IndexManager.build.either
          exists <- transact(sql"select to_regclass(${planned.name}) is not null".query[Boolean].selectOne)
        yield assertTrue(result.isLeft, exists.contains(true))
    },
    funcTest("retirement preserves a planned index even if its registry status is stale") {
      Given:
        Postgres.database >+> ProductionPool.layer(maxConnections = 1)
      Then:
        for
          _      <- setup *> IndexManager.build *> IndexManager.adopt
          _      <- transact(sql"update __rel_managed_index set status = 'retiring'".update)
          report <- IndexManager.retire
          exists <- transact(sql"select to_regclass(${planned.name}) is not null".query[Boolean].selectOne)
        yield assertTrue(report.contains("required by the active projection"), exists.contains(true))
    },
    funcTest("build waits for projection changes and cancellation releases its borrowed connection") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _ <- setup
          blocked <- ZIO.scoped {
            for
              env <- transaction.build
              _ <- ZIO.acquireRelease(
                sql"select 1 from pg_advisory_lock(${ProjectionRegistry.projectionLockKey})"
                  .query[Int]
                  .selectOne
                  .provideEnvironment(env)
              )(_ =>
                (env.get[ZConnection].rollback *>
                  sql"select pg_advisory_unlock(${ProjectionRegistry.projectionLockKey})"
                    .query[Boolean]
                    .selectOne
                    .provideEnvironment(env)).orDie
              )
              pending <- IndexManager.build.forkScoped
              waiting <- Postgres
                .query(
                  sql"""select exists (
                select 1 from pg_locks where locktype = 'advisory' and not granted
                  and database = (select oid from pg_database where datname = current_database()))"""
                    .query[Boolean]
                    .selectOne
                )
                .repeat(Schedule.recurUntil[Option[Boolean]](_.contains(true)).addDelay(_ => 20.millis))
                .timeoutFail(new RuntimeException("Index build did not wait on the projection lock"))(5.seconds)
              _ <- pending.interrupt
            yield waiting
          }
          _     <- IndexManager.build *> IndexManager.adopt
          after <- status(planned.name)
        yield assertTrue(blocked.contains(true), after.contains("active"))
    }
  )
