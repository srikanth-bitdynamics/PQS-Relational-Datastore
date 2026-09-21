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

object RecoverySpec extends SharedLedgerAndPostgresTest:
  private val note = DamlSource(
    "Note" -> """module Note where
                |
                |import Daml.Script
                |
                |template Note
                |  with
                |    owner : Party
                |    text : Text
                |  where
                |    signatory owner
                |
                |setup : Party -> Script ()
                |setup owner = do
                |  _ <- submit owner $ createCmd Note with owner, text = "n"
                |  pure ()
                |""".stripMargin
  )

  private val alice          = Party("Alice")
  private val templateFilter = s"(${note.name}:Note:Note)"

  private def ingest(start: String) =
    Pqs.runRelationalPipeline(
      "--pipeline-datasource=TransactionTreeStream",
      s"--pipeline-ledger-start=$start",
      "--pipeline-ledger-stop=Latest",
      s"--pipeline-filter-contracts=$templateFilter"
    )

  def spec = suite("relational recovery spec")(
    funcTest("restart cleanup removes partial rows past the watermark and replay does not duplicate"):
      Given:
        DamlSdk.dar(note) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      And:
        DamlSdk.runScript("Note:setup", alice.id)
      And:
        ingest("Genesis")
      When:
        // Simulate a crash that committed a transaction beyond the published watermark.
        Postgres.call(sql"insert into pqs_relational.__rel_transactions (tx_ix, ledger_offset) values (500, 999500)")
      And:
        DamlSdk.runScript("Note:setup", alice.id)
      And:
        ingest("Oldest")
      Then:
        FuncTest.retryUntilTimeout(
          Postgres.query(sql"""
            select 'contracts' as k, count(*)::text as v from pqs_relational.__rel_contracts
            union all select 'distinct_contracts', count(distinct contract_id)::text from pqs_relational.__rel_contracts
            union all select 'partial_rows', count(*)::text
              from pqs_relational.__rel_transactions where tx_ix = 500
            order by k
          """) `returns` table {
            "key"                | "count"
            ---                  | ---
            "contracts"          | "2"
            "distinct_contracts" | "2"
            "partial_rows"       | "0"
          }
        )
  )
end RecoverySpec
