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

object CoverageSpec extends SharedLedgerAndPostgresTest:
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

  private val templateFilter = s"(${note.name}:Note:Note)"

  private def ingest(datasource: String, start: String) =
    Pqs.runRelationalPipeline(
      s"--pipeline-datasource=$datasource",
      s"--pipeline-ledger-start=$start",
      "--pipeline-ledger-stop=Latest",
      s"--pipeline-filter-contracts=$templateFilter"
    )

  def spec = suite("relational coverage spec")(
    funcTest("the tree stream claims exercise history and tracks the committed offset") {
      val alice = Party("Alice")
      Given:
        DamlSdk.dar(note) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      And:
        DamlSdk.runScript("Note:setup", alice.id)
      And:
        ingest("TransactionTreeStream", "Genesis")
      Then:
        FuncTest.retryUntilTimeout(
          Postgres.query(sql"""
            select 'create_complete' as k, create_history_complete::text as v from pqs_relational.__query_coverage
            union all select 'exercise_complete', exercise_history_complete::text from pqs_relational.__query_coverage
            union all select 'tree_stream', tree_stream::text from pqs_relational.__query_coverage
            union all select 'through_tracks_watermark',
              (through_offset = (select ledger_offset from pqs_relational.__rel_watermark))::text
              from pqs_relational.__query_coverage
            order by k
          """) `returns` table {
            "key"                      | "value"
            ---                        | ---
            "create_complete"          | "true"
            "exercise_complete"        | "true"
            "through_tracks_watermark" | "true"
            "tree_stream"              | "true"
          }
        )
    },
    funcTest("the flat stream does not claim exercise history") {
      val alice = Party("Alice")
      Given:
        DamlSdk.dar(note) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      And:
        DamlSdk.runScript("Note:setup", alice.id)
      And:
        ingest("TransactionStream", "Genesis")
      Then:
        FuncTest.retryUntilTimeout(
          Postgres.query(sql"""
            select 'exercise_complete' as k, exercise_history_complete::text as v from pqs_relational.__query_coverage
            union all select 'tree_stream', tree_stream::text from pqs_relational.__query_coverage
            order by k
          """) `returns` table {
            "key"               | "value"
            ---                 | ---
            "exercise_complete" | "false"
            "tree_stream"       | "false"
          }
        )
    },
    funcTest("an acs seed does not extend a prior run's coverage") {
      val alice = Party("Alice")
      Given:
        DamlSdk.dar(note) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      And:
        // run one records a coverage row but ingests nothing — no matching contract exists yet
        ingest("TransactionTreeStream", "Genesis")
      When:
        // model a run that failed before its first checkpoint, so run two seeds from the ACS instead of resuming
        Postgres.call(sql"update pqs_relational.__rel_watermark set ledger_offset = null")
      And:
        DamlSdk.runScript("Note:setup", alice.id)
      And:
        ingest("TransactionTreeStream", "Latest")
      Then:
        FuncTest.retryUntilTimeout(
          Postgres.query(sql"""
            select 'coverage_rows' as k, count(*)::text as v from pqs_relational.__query_coverage
            union all select 'stale_not_extended',
              ((select through_offset from pqs_relational.__query_coverage where acs_seed_offset is null)
                is distinct from
               (select acs_seed_offset from pqs_relational.__query_coverage where acs_seed_offset is not null))::text
            union all select 'seed_tracks_offset',
              ((select through_offset from pqs_relational.__query_coverage where acs_seed_offset is not null)
                = (select acs_seed_offset from pqs_relational.__query_coverage where acs_seed_offset is not null))::text
            order by k
          """) `returns` table {
            "key"                | "value"
            ---                  | ---
            "coverage_rows"      | "2"
            "seed_tracks_offset" | "true"
            "stale_not_extended" | "true"
          }
        )
    }
  )
end CoverageSpec
