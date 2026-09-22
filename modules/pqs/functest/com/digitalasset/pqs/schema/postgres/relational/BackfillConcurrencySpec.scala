// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.schema.postgres.relational

import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.postgres.backend.transact
import com.digitalasset.pqs.postgres.relational.projection.*
import com.digitalasset.pqs.services.postgres.{Postgres, ProductionPool}
import com.digitalasset.transcode.schema.*
import zio.{Schedule, ZIO, durationInt}
import zio.jdbc.*
import zio.test.*

object BackfillConcurrencySpec extends FuncTest[Postgres]:
  def shared = Postgres.instance

  private val id =
    Identifier(PackageId("pkg"), PackageName("Test"), PackageVersion("1.0.0"), ModuleName("Main"), EntityName("Asset"))
  private val enumId =
    Identifier(PackageId("pkg"), PackageName("Test"), PackageVersion("1.0.0"), ModuleName("Main"), EntityName("State"))
  private def schema(cases: String*): Schema = Dictionary.make(
    Template(
      id,
      Descriptor.constructor(
        id,
        Descriptor.record(Seq("status" -> Descriptor.constructor(enumId, Descriptor.enumeration(cases.toSeq))))
      ),
      None,
      false,
      Seq.empty,
      Seq.empty
    )
  )
  private val config  = Map("asset" -> ProjectionDefinition(Seq("Test:Main:Asset"), Seq("status")))
  private val barrier = 9123456L

  def spec = suite("backfill concurrency")(
    funcTest("chunk commits retain the projection lock and enum extensions backfill without NULLs") {
      Given:
        Postgres.database >+> ProductionPool.layer(maxConnections = 5)
      Then:
        ZIO.scoped {
          for
            _ <- ProductionPool.relationalSchema
            base <- transact {
              for
                _ <- sql"call __rel_initialize_package('Test', '1.0.0', 'pkg')".execute
                _ <- sql"call __rel_initialize_entity('Test', 'Main', 'Asset', 'template')".execute
                _ <- ProjectionApply.apply(config, schema("A", "B"))
                entity <- sql"select pk, base_table from __rel_entity"
                  .query[(Long, String)]
                  .selectOne
                  .someOrFail(new RuntimeException("no entity"))
                (pk, table) = entity
                _ <- sql"insert into __rel_transactions (tx_ix, ledger_offset) values (1, 100), (2, 200)".update
                _ <- sql"""insert into __rel_contracts
                     (contract_pk, contract_id, template_entity_pk, representative_package_id, created_tx_ix, source_kind)
                     values (1, 'c1', $pk, 'pkg', 1, 'stream'), (2, 'c2', $pk, 'pkg', 2, 'stream')""".update
                _ <- SqlFragment(
                  s"""insert into "$table" (contract_pk, created_tx_ix, payload_json)
                      values (1, 1, '{"status":"C"}'), (2, 2, '{"status":"C"}')"""
                ).update
                _ <- sql"update __rel_watermark set ledger_offset = 200, tx_ix = 2".update
                _ <- SqlFragment(
                  s"""create function pause_second_row() returns trigger language plpgsql as $$$$
                        begin if new.contract_pk = 2 then perform pg_advisory_xact_lock($barrier); end if; return new; end; $$$$;
                        create trigger pause_second before update on "$table" for each row execute function pause_second_row();"""
                ).execute
              yield table
            }
            stored <- transact(ProjectionRegistry.resolvedShapeOf(1L)).someOrFail(new RuntimeException("no shape"))
            shapes = ProjectionBinding.parse(stored)
            blocker <- transaction.build
            _ <- ZIO.acquireRelease(
              sql"select 1 from pg_advisory_lock($barrier)".query[Int].selectOne.provideEnvironment(blocker)
            )(_ =>
              (sql"select pg_advisory_unlock($barrier)".query[Boolean].selectOne.provideEnvironment(blocker) *>
                blocker.get[ZConnection].access(_.commit())).orDie
            )
            backfill <- transact(
              sql"set statement_timeout = '15s'".execute *>
                ProjectionBackfill.runAndPublish(schema("A", "B", "C"), shapes, 1L, 1)
            ).forkScoped
            _ <- transact(
              sql"select exists (select 1 from pg_locks where locktype = 'advisory' and objid = $barrier::oid and not granted)"
                .query[Boolean]
                .selectOne
            )
              .repeat(Schedule.recurUntil[Option[Boolean]](_.contains(true)).addDelay(_ => 20.millis))
              .timeoutFail(new RuntimeException("backfill did not reach its second chunk"))(10.seconds)
            first <- transact(
              SqlFragment(s"select status from \"$base\" where contract_pk = 1").query[String].selectOne
            )
            apply <- transact(
              sql"set local lock_timeout = '200ms'".execute *>
                ProjectionApply.apply(Map.empty, schema("A", "B", "C"))
            ).either
            activate <- transact(
              sql"set local lock_timeout = '200ms'".execute *> ProjectionRegistry.activate(1L)
            ).either
            duplicate <- transact(ProjectionBackfill.runAndPublish(schema("A", "B", "C"), shapes, 1L, 1)).either
            _         <- sql"select pg_advisory_unlock($barrier)".query[Boolean].selectOne.provideEnvironment(blocker)
            through   <- backfill.join
            _         <- transact(ProjectionRegistry.activate(1L))
            rows <- transact(SqlFragment(s"select status from \"$base\" order by contract_pk").query[String].selectAll)
          yield assertTrue(
            first.contains("C"),
            apply.isLeft,
            activate.isLeft,
            duplicate.isLeft,
            through == 2L,
            rows.toSeq == Seq("C", "C")
          )
        }
    }
  )
