package com.digitalasset.pqs.postgres.relational

import com.digitalasset.canonical.{ContractId, Party}
import com.digitalasset.pqs.postgres.backend.IdPlaceholder
import com.digitalasset.transcode.schema.ChoiceName
import zio.test.*

object RelationalModelSpec extends ZIOSpecDefault:

  private def factory = IdPlaceholder.factory(0L)

  private def event(pk: IdPlaceholder) = model.Event(
    specific.Event(
      pk = pk,
      txIx = 5L,
      ledgerOffset = 100L,
      nodeId = 2,
      contractId = ContractId("cid-1"),
      templateEntityPk = 7L,
      eventKind = model.EventKind.Create,
      sourceKind = model.SourceKind.Stream,
      archiveSource = None,
      visibilityComplete = true
    )
  )

  private def contract(pk: IdPlaceholder) = model.Contract(
    specific.Contract(
      contractPk = pk,
      contractId = ContractId("cid-1"),
      templateEntityPk = 7L,
      representativePackageId = "pkg-1",
      creationPackageId = None,
      createdAtIx = 5L,
      createdAtOffset = Some(100L),
      signatories = Seq(Party("Alice"), Party("Bob")),
      observers = Seq(Party("Carol")),
      createWitnesses = Seq.empty,
      metadata = Some(Array[Byte](1, 15)),
      contractKey = Some(ujson.Str("k")),
      contractKeyHash = None,
      acsDelta = true,
      sourceKind = model.SourceKind.Stream,
      synchronizerId = Some("sync-1")
    )
  )

  def spec = suite("relational model")(
    test("COPY headers carry the /*N*/ order, table names and columns matching V001"):
      val f = factory
      val headers = Seq(
        model.Transaction(
          specific.Transaction(
            1L,
            com.digitalasset.canonical.specific.Offset.Absolute(100L),
            None,
            None,
            None,
            None,
            None,
            None
          )
        )._sql,
        event(f.mk)._sql,
        model.EventVisibility(specific.EventVisibility(f.mk, Party("Alice")))._sql,
        contract(f.mk)._sql,
        model.ContractVisibility(specific.ContractVisibility(f.mk, Party("Alice"), model.VisibilityRole.Signatory))._sql,
        model.Exercise(
          specific.Exercise(f.mk, 8L, 7L, ChoiceName("Transfer"), true, Seq(Party("Alice")), ujson.Null, ujson.Null, 4)
        )._sql,
        model.TmpLifecycle(specific.TmpLifecycle(ContractId("cid-1"), 5L, Some(100L)))._sql
      )
      assertTrue(
        headers(0) ==
          "/*0*/ copy __rel_transactions (tx_ix, ledger_offset, transaction_id, effective_at, synchronizer_id, workflow_id, external_transaction_hash, paid_traffic_cost) from stdin",
        headers(1) ==
          "/*1*/ copy __query_events (event_pk, tx_ix, ledger_offset, node_id, contract_id, template_entity_pk, event_kind, source_kind, archive_source, visibility_complete) from stdin",
        headers(2) ==
          "/*2*/ copy __query_event_visibility (event_pk, party) from stdin",
        headers(3) ==
          "/*3*/ copy __rel_contracts (contract_pk, contract_id, template_entity_pk, representative_package_id, creation_package_id, created_tx_ix, created_at_offset, signatories, observers, create_witnesses, creation_synchronizer_id, metadata, contract_key_json, contract_key_hash, divulged_only, source_kind) from stdin",
        headers(4) ==
          "/*4*/ copy __rel_contract_visibility (contract_pk, party, role) from stdin",
        headers(5) ==
          "/*5*/ copy __rel_exercises (event_pk, choice_entity_pk, contract_template_entity_pk, choice_name, consuming, controllers, argument_json, result_json, last_descendant_node_id) from stdin",
        headers(6) ==
          "/*6*/ copy __rel_tmp_lifecycle (contract_id, archived_tx_ix, archived_at_offset) from stdin"
      )
    ,
    test("event row is tab-separated with enum label and \\N for a missing archive source"):
      assertTrue(event(factory.mk)._row == "1\t5\t100\t2\tcid-1\t7\tcreate\tstream\t\\N\ttrue")
    ,
    test("contract row encodes arrays, options, bytea, jsonb and divulged_only"):
      assertTrue(
        contract(factory.mk)._row ==
          "1\tcid-1\t7\tpkg-1\t\\N\t5\t100\t{Alice,Bob}\t{Carol}\t{}\tsync-1\t\\\\x010F\t\"k\"\t\\N\tfalse\tstream"
      )
    ,
    test("IdPlaceholder.factory allocates consecutive ids for events and contracts"):
      val f = factory
      assertTrue(f.mk.id == 1L, f.mk.id == 2L, f.mk.id == 3L)
    ,
    test("payload and interface-view rows target the resolved dynamic table name"):
      val f       = factory
      val payload = model.ContractPayload(specific.ContractPayload(f.mk, ujson.Str("x")), "rel_asset__asset__asset")
      val view    = model.InterfaceView(specific.InterfaceView(f.mk, ujson.Str("v")), "relv_iface__iface__iface")
      assertTrue(
        payload._sql == "/*7*/ copy rel_asset__asset__asset (contract_pk, payload_json) from stdin",
        payload._row == "1\t\"x\"",
        view._sql == "/*8*/ copy relv_iface__iface__iface (contract_pk, view_json) from stdin",
        view._row == "2\t\"v\""
      )
  )
