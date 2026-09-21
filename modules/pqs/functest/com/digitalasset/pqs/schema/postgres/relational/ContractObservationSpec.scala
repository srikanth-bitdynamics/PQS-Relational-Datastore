// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.schema.postgres.relational

import com.digitalasset.canonical.{ContractId, Party}
import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.postgres.backend.{IdPlaceholder, transact}
import com.digitalasset.pqs.postgres.relational.{ContractObservations, model, specific}
import com.digitalasset.pqs.services.postgres.{Postgres, ProductionPool}
import zio.{Chunk, ZIO}
import zio.jdbc.*
import zio.test.*

object ContractObservationSpec extends FuncTest[Postgres]:
  def shared = Postgres.instance

  private val setup = ProductionPool.relationalSchema *> transact(sql"""
    insert into __rel_entity(pk,package_name,module_name,entity_name,kind,base_table)
      values (1,'Test','Main','Asset','template','payload');
    create table payload(contract_pk bigint primary key references __rel_contracts on delete cascade,
      created_tx_ix bigint not null, archived_tx_ix bigint, payload_json jsonb not null);
    insert into __rel_transactions(tx_ix,ledger_offset) values (1,10),(2,20),(3,30);
  """.execute)

  private def observation(pk: Long, ix: Long, kind: model.SourceKind): Chunk[model.Model] =
    val id = IdPlaceholder.factory(pk - 1).mk
    val fromAssignment = kind match
      case model.SourceKind.Assignment => true
      case _                           => false
    Chunk(
      model.Contract(
        specific.Contract(
          id,
          ContractId("contract"),
          1L,
          "pkg",
          None,
          ix,
          if fromAssignment then None else Some(ix * 10),
          Seq.empty,
          Seq.empty,
          Seq.empty,
          None,
          None,
          None,
          true,
          kind,
          None,
          fromAssignment
        )
      ),
      model.ContractPayload(specific.ContractPayload(id, ix, ujson.Obj("secret" -> "value")), "payload"),
      model.ContractVisibility(specific.ContractVisibility(id, Party(s"Witness-$ix"), model.VisibilityRole.Witness))
    )

  private def write(rows: Chunk[model.Model]) =
    transact(
      ContractObservations
        .prepare(rows)
        .flatMap(normalized =>
          model.Model.prepareStatement(normalized, model.statTables).tap(_ => ContractObservations.finish(rows))
        )
    )

  def spec = suite("contract observations")(
    funcTest("assignment introduces state without a create offset and repeated observations keep one payload") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _ <- setup
          _ <- write(observation(1, 1, model.SourceKind.Assignment))
          _ <- write(observation(2, 2, model.SourceKind.Assignment))
          actual <- transact(
            sql"select contract_pk, created_tx_ix, created_at_offset is null, history_lower_bound from __rel_contracts"
              .query[(Long, Long, Boolean, Boolean)]
              .selectAll
          )
          payloads    <- transact(sql"select count(*) from payload".query[Long].selectOne)
          unpublished <- transact(sql"select count(*) from __rel_contract_visibility".query[Long].selectOne)
          _           <- transact(sql"update __rel_watermark set tx_ix=2, ledger_offset=20".execute)
          visibility  <- transact(sql"select count(*) from __rel_contract_visibility".query[Long].selectOne)
        yield assertTrue(
          actual.toSeq == Seq((1L, 1L, true, true)),
          payloads.contains(1L),
          visibility.contains(2L),
          unpublished.contains(0L)
        )
    },
    funcTest("parallel batches observe one identity and retain the earliest boundary") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _ <- setup
          _ <- ZIO.collectAllPar(
            Seq(
              write(observation(2, 2, model.SourceKind.Assignment)),
              write(observation(1, 1, model.SourceKind.Stream))
            )
          )
          actual <- transact(
            sql"select created_tx_ix, created_at_offset, history_lower_bound from __rel_contracts"
              .query[(Long, Long, Boolean)]
              .selectAll
          )
          payloads <- transact(sql"select count(*) from payload".query[Long].selectOne)
          diverged <- transact(
            sql"""select count(*) from payload p join __rel_contracts c using (contract_pk)
                  where p.created_tx_ix is distinct from c.created_tx_ix
                     or p.archived_tx_ix is distinct from c.archived_tx_ix""".query[Long].selectOne
          )
          materialized <- transact(sql"select created_tx_ix from payload".query[Long].selectOne)
        yield assertTrue(
          actual.toSeq == Seq((1L, 10L, false)),
          payloads.contains(1L),
          materialized.contains(1L),
          diverged.contains(0L)
        )
    },
    funcTest("archive publication materialises the lifecycle boundary onto the payload row") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _ <- setup
          _ <- write(observation(1, 1, model.SourceKind.Stream))
          _ <- transact(
            sql"""insert into __rel_tmp_lifecycle(contract_id, archived_tx_ix, archived_at_offset)
                  values ('contract', 2, 20)""".execute
          )
          before <- transact(sql"select archived_tx_ix is null from payload".query[Boolean].selectOne)
          _      <- transact(sql"update __rel_watermark set tx_ix=2, ledger_offset=20".execute)
          after <- transact(
            sql"""select p.archived_tx_ix, c.archived_tx_ix from payload p
                  join __rel_contracts c using (contract_pk)""".query[(Long, Long)].selectOne
          )
        yield assertTrue(before.contains(true), after.contains((2L, 2L)))
    },
    funcTest("an assignment-created contract materialises its archive boundary") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _ <- setup
          _ <- write(observation(1, 1, model.SourceKind.Assignment))
          _ <- transact(
            sql"""insert into __rel_tmp_lifecycle(contract_id, archived_tx_ix, archived_at_offset)
                  values ('contract', 2, 20)""".execute
          )
          _ <- transact(sql"update __rel_watermark set tx_ix=2, ledger_offset=20".execute)
          actual <- transact(
            sql"""select p.archived_tx_ix, c.archived_tx_ix from payload p
                  join __rel_contracts c using (contract_pk)""".query[(Long, Long)].selectOne
          )
        yield assertTrue(actual.contains((2L, 2L)))
    },
    funcTest("an archive observed before its create materialises once the create arrives") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _ <- setup
          _ <- transact(
            sql"""insert into __rel_tmp_lifecycle(contract_id, archived_tx_ix, archived_at_offset)
                  values ('contract', 2, 20)""".execute
          )
          _      <- transact(sql"update __rel_watermark set tx_ix=2, ledger_offset=20".execute)
          staged <- transact(sql"select count(*) from __rel_tmp_lifecycle".query[Long].selectOne)
          _      <- write(observation(1, 1, model.SourceKind.Stream))
          _      <- transact(sql"update __rel_watermark set tx_ix=3, ledger_offset=30".execute)
          actual <- transact(
            sql"""select p.archived_tx_ix, c.archived_tx_ix from payload p
                  join __rel_contracts c using (contract_pk)""".query[(Long, Long)].selectOne
          )
          drained <- transact(sql"select count(*) from __rel_tmp_lifecycle".query[Long].selectOne)
        yield assertTrue(staged.contains(1L), actual.contains((2L, 2L)), drained.contains(0L))
    },
    funcTest("a failed watermark advance leaves canonical and materialised lifecycle unchanged") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _ <- setup
          _ <- write(observation(1, 1, model.SourceKind.Stream))
          _ <- transact(
            sql"""insert into __rel_tmp_lifecycle(contract_id, archived_tx_ix, archived_at_offset)
                  values ('contract', 2, 20)""".execute
          )
          failed <- transact(
            sql"update __rel_watermark set tx_ix=2, ledger_offset=20".execute *>
              ZIO.fail(new RuntimeException("interrupted before commit"))
          ).either
          after <- transact(
            sql"""select (select count(*) from payload where archived_tx_ix is not null),
                         (select count(*) from __rel_contracts where archived_tx_ix is not null)"""
              .query[(Long, Long)]
              .selectOne
          )
          _ <- transact(sql"update __rel_watermark set tx_ix=2, ledger_offset=20".execute)
          recovered <- transact(
            sql"""select p.archived_tx_ix, c.archived_tx_ix from payload p
                  join __rel_contracts c using (contract_pk)""".query[(Long, Long)].selectOne
          )
        yield assertTrue(failed.isLeft, after.contains((0L, 0L)), recovered.contains((2L, 2L)))
    },
    funcTest("recovery resets the materialised archive boundary with the canonical one") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _ <- setup
          _ <- write(observation(1, 1, model.SourceKind.Stream))
          _ <- transact(
            sql"""insert into __rel_tmp_lifecycle(contract_id, archived_tx_ix, archived_at_offset)
                  values ('contract', 3, 30)""".execute
          )
          _ <- transact(sql"update __rel_watermark set tx_ix=3, ledger_offset=30".execute)
          archived <- transact(
            sql"""select p.archived_tx_ix, c.archived_tx_ix from payload p
                  join __rel_contracts c using (contract_pk)""".query[(Long, Long)].selectOne
          )
          _ <- transact(sql"call __rel_delete_transactions_after(2)".execute)
          reset <- transact(
            sql"""select p.archived_tx_ix is null, c.archived_tx_ix is null from payload p
                  join __rel_contracts c using (contract_pk)""".query[(Boolean, Boolean)].selectOne
          )
          diverged <- transact(
            sql"""select count(*) from payload p join __rel_contracts c using (contract_pk)
                  where p.created_tx_ix is distinct from c.created_tx_ix
                     or p.archived_tx_ix is distinct from c.archived_tx_ix""".query[Long].selectOne
          )
        yield assertTrue(archived.contains((3L, 3L)), reset.contains((true, true)), diverged.contains(0L))
    },
    funcTest("a delayed exercise cannot restore a redacted contract payload") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _ <- setup
          _ <- write(observation(1, 1, model.SourceKind.Stream))
          _ <- transact(
            sql"""update __rel_contracts set archived_tx_ix=2, archived_at_offset=20;
            update __rel_watermark set tx_ix=3, ledger_offset=30""".execute *> sql"select redact_contract('contract','request')"
              .query[Long]
              .selectOne
          )
          _ <- transact(
            sql"""insert into __query_events(event_pk,tx_ix,ledger_offset,node_id,contract_id,template_entity_pk,event_kind,source_kind,visibility_complete)
              values (9,3,30,1,'contract',1,'exercise','stream',true);
            insert into __rel_exercises(event_pk,choice_entity_pk,contract_template_entity_pk,choice_name,consuming,controllers,argument_json,result_json,last_descendant_node_id)
              values (9,null,1,'Read',false,'{}','{"secret":1}','{"secret":2}',1)
            """.execute *>
              ContractObservations.finish(
                Chunk(
                  model.Event(
                    specific.Event(
                      IdPlaceholder.factory(8).mk,
                      3,
                      30,
                      1,
                      ContractId("contract"),
                      1,
                      model.EventKind.Exercise,
                      model.SourceKind.Stream,
                      None,
                      true
                    )
                  )
                )
              )
          )
          erased <- transact(sql"""select argument_json is null, result_json is null, redaction_id
            from __rel_exercises where event_pk=9""".query[(Boolean, Boolean, String)].selectOne)
        yield assertTrue(erased.contains((true, true, "request")))
    },
    funcTest("redaction and pruning tombstones prevent later assignments from restoring payloads") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _ <- setup
          _ <- write(observation(1, 1, model.SourceKind.Stream))
          _ <- transact(
            sql"""update __rel_contracts set archived_tx_ix=2, archived_at_offset=20;
            update __rel_watermark set tx_ix=3, ledger_offset=30""".execute *>
              sql"select redact_contract('contract','request')".query[Long].selectOne
          )
          _        <- write(observation(2, 2, model.SourceKind.Assignment))
          redacted <- transact(sql"select count(*) from payload".query[Long].selectOne)
          _ <- transact(
            sql"select * from prune_archived_to_offset(20)"
              .query[(Option[Long], Long, Long, Long, Long)]
              .selectOne
          )
          _ <- write(observation(3, 3, model.SourceKind.Assignment))
          after <- transact(
            sql"select (select count(*) from payload), (select count(*) from __rel_contracts)"
              .query[(Long, Long)]
              .selectOne
          )
        yield assertTrue(redacted.contains(0L), after.contains((0L, 0L)))
    }
  )
