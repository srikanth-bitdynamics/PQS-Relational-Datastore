package com.digitalasset.pqs.schema.postgres.relational

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.{DamlSdk, DamlSource, Party}
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.Pqs
import zio.jdbc.*
import zio.test.*

import scala.language.{implicitConversions, postfixOps}

/** Reference-model recovery test: an interrupted, resumed ingestion must produce the same logical state as a single
  * uninterrupted ingestion of the identical ledger. The snapshot is keyed by ledger-stable identifiers (contract id,
  * offset:node) and excludes run-dependent values (primary keys, per-run coverage rows), so any divergence is caught.
  */
object RecoveryEquivalenceSpec extends SharedLedgerAndPostgresTest:
  private val recov = DamlSource(
    "Recov" -> """module Recov where
                 |
                 |import Daml.Script
                 |import DA.Foldable (forA_)
                 |
                 |template Item
                 |  with
                 |    owner : Party
                 |    tag : Text
                 |  where
                 |    signatory owner
                 |    choice Retire : ()
                 |      controller owner
                 |      do pure ()
                 |
                 |build1 : Party -> Script ()
                 |build1 owner = do
                 |  _ <- submit owner $ createCmd Item with owner, tag = "a"
                 |  _ <- submit owner $ createCmd Item with owner, tag = "b"
                 |  pure ()
                 |
                 |build2 : Party -> Script ()
                 |build2 owner = do
                 |  items <- query @Item owner
                 |  forA_ (filter (\(_, i) -> i.tag == "a") items) (\(cid, _) -> submit owner $ exerciseCmd cid Retire)
                 |  _ <- submit owner $ createCmd Item with owner, tag = "c"
                 |  pure ()
                 |""".stripMargin
  )

  private val templateFilter = s"(${recov.name}:Recov:Item)"

  private def ingest(start: String) =
    Pqs.runRelationalPipeline(
      "--pipeline-datasource=TransactionTreeStream",
      s"--pipeline-ledger-start=$start",
      "--pipeline-ledger-stop=Latest",
      s"--pipeline-filter-contracts=$templateFilter"
    )

  // A run-independent projection of the ingested logical state: contract lifecycle + parties, events by ledger
  // coordinate, and visibility by contract id. Ordered so two runs of the same ledger produce byte-identical tables.
  private val snapshot = sql"""
    select 'C' as t, contract_id as k,
           (case when archived_tx_ix is null then 'active' else 'archived' end)
             || '|' || array_to_string(signatories, ',')
             || '|' || array_to_string(observers, ',') as v
    from pqs_relational.__rel_contracts
    union all
    select 'E', ledger_offset::text || ':' || node_id::text,
           event_kind::text || '|' || coalesce(archive_source::text, '-')
    from pqs_relational.__query_events
    union all
    select 'V', c.contract_id || '|' || vis.party || '|' || vis.role::text, '1'
    from pqs_relational.__rel_contract_visibility vis
    join pqs_relational.__rel_contracts c on vis.contract_pk = c.contract_pk
    order by t, k
  """

  def spec = suite("relational recovery-equivalence spec")(
    funcTest("an interrupted, resumed run yields the same logical state as an uninterrupted run") {
      val alice = Party("Alice")
      val ref   = Capture[Table]
      Given:
        DamlSdk.dar(recov) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      And:
        DamlSdk.runScript("Recov:build1", alice.id)
      And:
        // interrupted run, pass 1: ingest the first two creates
        ingest("Genesis")
      And:
        DamlSdk.runScript("Recov:build2", alice.id)
      And:
        // interrupted run, pass 2: resume and ingest the archive and third create
        ingest("Oldest")
      Then:
        FuncTest.retryUntilTimeout(Postgres.query(snapshot) `is` ref.capture)
      When:
        // discard the datastore; the ledger is now complete, so the next run ingests it uninterrupted
        Postgres.call(sql"drop schema pqs_relational cascade")
      And:
        ingest("Genesis")
      Expect:
        FuncTest.retryUntilTimeout(Postgres.query(snapshot) `returns` ref.get)
    },
    funcTest("rows committed past the watermark by a crash are discarded, converging on the uninterrupted state") {
      val alice = Party("Alice")
      val ref   = Capture[Table]
      Given:
        DamlSdk.dar(recov) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      And:
        DamlSdk.runScript("Recov:build1", alice.id)
      And:
        DamlSdk.runScript("Recov:build2", alice.id)
      And:
        ingest("Genesis")
      When:
        // simulate a crash that committed a transaction and its contract beyond the published watermark
        Postgres.query {
          sql"insert into pqs_relational.__rel_transactions (tx_ix, ledger_offset) values (500, 999500)".execute
            *> sql"""insert into pqs_relational.__rel_contracts
                       (contract_pk, contract_id, template_entity_pk, representative_package_id, created_tx_ix,
                        source_kind)
                     values (999500, 'stray-cid', (select pk from pqs_relational.__rel_entity
                       where entity_name = 'Item' and kind = 'template' limit 1), 'stray-pkg', 500, 'stream')""".execute
        }
      And:
        // restart: cleanup discards everything past the watermark before resuming
        ingest("Oldest")
      Then:
        FuncTest.retryUntilTimeout(Postgres.query(snapshot) `is` ref.capture)
      When:
        Postgres.call(sql"drop schema pqs_relational cascade")
      And:
        ingest("Genesis")
      Expect:
        FuncTest.retryUntilTimeout(Postgres.query(snapshot) `returns` ref.get)
    }
  )
end RecoveryEquivalenceSpec
