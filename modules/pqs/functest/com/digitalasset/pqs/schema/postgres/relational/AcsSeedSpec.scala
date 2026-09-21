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

object AcsSeedSpec extends SharedLedgerAndPostgresTest:
  private val asset = DamlSource(
    "Asset" -> """module Asset where
                 |
                 |import Daml.Script
                 |
                 |template Asset
                 |  with
                 |    owner : Party
                 |    amount : Int
                 |  where
                 |    signatory owner
                 |
                 |setup : Party -> Script ()
                 |setup owner = do
                 |  _ <- submit owner $ createCmd Asset with owner, amount = 100
                 |  pure ()
                 |""".stripMargin
  )

  private val alice          = Party("Alice")
  private val templateFilter = s"(${asset.name}:Asset:Asset)"

  def spec = suite("relational acs-seed spec")(
    funcTest("seeds the ACS with source_kind acs_seed and records acs coverage"):
      Given:
        DamlSdk.dar(asset) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      And:
        DamlSdk.runScript("Asset:setup", alice.id)
      And:
        // Fresh datastore + a non-Genesis start makes the pipeline rehydrate from the ACS (processAcs).
        Pqs.runRelationalPipeline(
          "--pipeline-datasource=TransactionTreeStream",
          "--pipeline-ledger-start=Latest",
          "--pipeline-ledger-stop=Latest",
          s"--pipeline-filter-contracts=$templateFilter"
        )
      Then:
        FuncTest.retryUntilTimeout(
          Postgres.query(sql"""
            select 'acs_contracts' as k, count(*)::text as v
              from pqs_relational.__rel_contracts where source_kind = 'acs_seed'
            union all select 'null_created_offset', count(*)::text
              from pqs_relational.__rel_contracts where source_kind = 'acs_seed' and created_at_offset is null
            union all select 'acs_create_events', count(*)::text
              from pqs_relational.__query_events where event_kind = 'create' and source_kind = 'acs_seed'
            union all select 'genesis_tx', count(*)::text
              from pqs_relational.__rel_transactions where tx_ix = 0 and ledger_offset > 0
            union all select 'acs_coverage', count(*)::text
              from pqs_relational.__query_coverage
              where acs_seed_offset is not null and tree_stream = true and reassignment_history_complete = false
            order by k
          """) `returns` table {
            "key"                 | "count"
            ---                   | ---
            "acs_contracts"       | "1"
            "acs_coverage"        | "1"
            "acs_create_events"   | "1"
            "genesis_tx"          | "1"
            "null_created_offset" | "1"
          }
        )
  )
end AcsSeedSpec
