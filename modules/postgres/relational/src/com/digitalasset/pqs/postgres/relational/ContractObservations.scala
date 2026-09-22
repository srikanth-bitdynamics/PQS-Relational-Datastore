// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.relational

import com.digitalasset.pqs.utils.safeequals.===
import zio.{Chunk, ZIO}
import zio.jdbc.*

/** An assignment can introduce a contract or repeat one already observed through a create or ACS. */
private[pqs] object ContractObservations:
  def prepare(rows: Chunk[model.Model]): ZIO[ZConnection, Throwable, Chunk[model.Model]] =
    val observations = rows
      .collect { case c: model.Contract => c.ev }
      .sortBy(_.createdAtIx)
      .distinctBy(_.contractId)
    if observations.isEmpty then ZIO.succeed(rows)
    else
      val ids = ujson.write(ujson.Arr.from(observations.map(c => ujson.Str(c.contractId))))
      for
        // Sorted transaction locks serialize observations of the same contract across parallel COPY batches.
        _ <- sql"""select 1 from (
                     select pg_advisory_xact_lock(k) from (
                       select distinct hashtextextended(value, 749136) as k
                       from jsonb_array_elements_text($ids::jsonb) order by k
                     ) keys
                   ) locks""".query[Int].selectAll
        existing <- sql"""select contract_id from __rel_contracts
                           where contract_id in (select jsonb_array_elements_text($ids::jsonb))
                           union select contract_id from __rel_contract_tombstone
                           where contract_id in (select jsonb_array_elements_text($ids::jsonb))
                           union select contract_id from __rel_redaction
                           where contract_id in (select jsonb_array_elements_text($ids::jsonb))"""
          .query[String]
          .selectAll
        // A later batch may have committed first. Retain the earliest observed boundary without replacing payloads.
        bounds = ujson.write(
          ujson.Arr.from(
            observations.map(c =>
              ujson.Obj(
                "contract_id"   -> ujson.Str(c.contractId),
                "tx_ix"         -> ujson.Str(c.createdAtIx.toString),
                "ledger_offset" -> c.createdAtOffset.fold[ujson.Value](ujson.Null)(v => ujson.Str(v.toString)),
                "source"        -> model.sourceKindConverter.convert(c.sourceKind),
                "lower_bound"   -> c.historyLowerBound,
                "synchronizer"  -> c.synchronizerId.fold[ujson.Value](ujson.Null)(ujson.Str(_))
              )
            )
          )
        )
        _ <- sql"call __rel_apply_observation_bounds($bounds::jsonb)".execute
        existingIds = existing.toSet
        retained    = observations.filterNot(c => existingIds.contains(c.contractId)).map(_.contractPk.id).toSet
      yield rows.filter {
        case c: model.Contract           => retained.contains(c.ev.contractPk.id)
        case _: model.ContractVisibility => false
        case c: model.ContractPayload    => retained.contains(c.ev.contractPk.id)
        case c: model.InterfaceView      => retained.contains(c.ev.contractPk.id)
        case _                           => true
      }

  /** Stage visibility by stable contract identity; the watermark publishes it with the corresponding events. */
  def finish(rows: Chunk[model.Model]): ZIO[ZConnection, Throwable, Unit] =
    val contracts = rows.collect { case c: model.Contract => c.ev.contractPk.id -> c.ev }.toMap
    val visibility = rows.collect { case c: model.ContractVisibility => c.ev }.flatMap { v =>
      contracts.get(v.contractPk.id).map { id =>
        ujson.Obj(
          "contract_id" -> ujson.Str(id.contractId),
          "tx_ix"       -> ujson.Str(id.createdAtIx.toString),
          "party"       -> ujson.Str(v.party),
          "role"        -> model.visibilityRoleConverter.convert(v.role)
        )
      }
    }
    val witnesses = ujson.write(ujson.Arr.from(visibility))
    val events = rows.collect {
      case e: model.Event if e.ev.eventKind === model.EventKind.Exercise => ujson.Str(e.ev.pk.id.toString)
    }
    val eventIds = ujson.write(ujson.Arr.from(events))
    val merge = sql"""insert into __rel_pending_visibility(contract_id, party, role, tx_ix)
                       select v.contract_id, v.party, v.role::rel_visibility_role, v.tx_ix
                       from jsonb_to_recordset($witnesses::jsonb) as v(contract_id text, party text, role text, tx_ix bigint)
                       join __rel_contracts c using (contract_id) where c.redaction_id is null
                       on conflict do nothing""".update
    // A delayed event must not restore values covered by an earlier redaction request.
    val redact = sql"""update __rel_exercises x set argument_json = null, result_json = null,
                        redaction_id = r.redaction_id
                      from __query_events e join __rel_redaction r
                        on r.contract_id = e.contract_id
                        or (r.event_id_offset = e.ledger_offset and r.event_id_node = e.node_id)
                      where x.event_pk = e.event_pk and x.redaction_id is null
                        and e.event_pk in (select value::bigint from jsonb_array_elements_text($eventIds::jsonb))""".update
    ZIO.when(visibility.nonEmpty)(merge) *> ZIO.when(events.nonEmpty)(redact).unit
