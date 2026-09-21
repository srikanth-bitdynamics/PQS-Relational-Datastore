package com.digitalasset.pqs.schema.postgres.relational

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.{DamlSdk, DamlSource, Party}
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.Pqs
import zio.ExitCode
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

  private val templateFilter = s"(${note.name}:Note:Note)"

  private def ingest(start: String) =
    Pqs.runRelationalPipeline(
      "--pipeline-datasource=TransactionTreeStream",
      s"--pipeline-ledger-start=$start",
      "--pipeline-ledger-stop=Latest",
      s"--pipeline-filter-contracts=$templateFilter"
    )

  def spec = suite("relational recovery spec")(
    funcTest("restart cleanup removes partial rows past the watermark and replay does not duplicate") {
      val alice = Party("Alice")
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
    },
    funcTest("an interrupted acs seed with no published watermark is wiped and re-seeded on restart") {
      val alice = Party("Alice")
      Given:
        DamlSdk.dar(note) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      And:
        DamlSdk.runScript("Note:setup", alice.id)
      And:
        // a non-Genesis start rehydrates current state from the ACS into tx_ix = 0
        ingest("Latest")
      When:
        // reproduce the pre-publish crash: clearing only ledger_offset empties latest_checkpoint() while the
        // seed rows survive (updating tx_ix would trip the `before update of tx_ix` trigger)
        Postgres.call(sql"update pqs_relational.__rel_watermark set ledger_offset = null")
      And:
        // on restart the missing checkpoint drives cutoff -1, wiping the seed (incl. tx_ix = 0) before a clean re-seed
        ingest("Latest")
      Then:
        FuncTest.retryUntilTimeout(
          Postgres.query(sql"""
            select 'acs_contracts' as k, count(*)::text as v
              from pqs_relational.__rel_contracts where source_kind = 'acs_seed'
            union all select 'distinct_contracts', count(distinct contract_id)::text
              from pqs_relational.__rel_contracts
            union all select 'genesis_tx', count(*)::text
              from pqs_relational.__rel_transactions where tx_ix = 0
            order by k
          """) `returns` table {
            "key"                | "count"
            ---                  | ---
            "acs_contracts"      | "1"
            "distinct_contracts" | "1"
            "genesis_tx"         | "1"
          }
        )
    },
    funcTest("a stale writer instance cannot take over and advance the watermark") {
      val alice = Party("Alice")
      Given:
        DamlSdk.dar(note) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      And:
        DamlSdk.runScript("Note:setup", alice.id)
      And:
        ingest("Genesis")
      And:
        // spoil the writer identity, then keep forcing it so the restarting pipeline cannot reclaim it
        Postgres.query(
          sql"update pqs_relational.__rel_watermark set instance_id = 'another-instance'".update.returns(1)
        )
      And:
        Postgres
          .query {
            sql"""
              create function override_rel_instance_id()
              returns trigger
              language plpgsql
              as $$$$
              begin
                  new.instance_id := 'another-instance';
                  return new;
              end;
              $$$$;

              create trigger trg_override_rel_instance_id
              before update on pqs_relational.__rel_watermark
              for each row
              execute function override_rel_instance_id();
             """.update
          }
          .returns(0)
      And:
        DamlSdk.runScript("Note:setup", alice.id)
      And:
        Pqs.attemptRelationalPipeline(
          "--pipeline-datasource=TransactionTreeStream",
          "--pipeline-ledger-start=Oldest",
          "--pipeline-ledger-stop=Latest",
          s"--pipeline-filter-contracts=$templateFilter"
        )
      And:
        Pqs.exitCode.is(ExitCode.failure)
      And:
        Pqs.stderr.is(stringContaining("PQS writer instance has changed") && stringContaining("another-instance"))
      Expect:
        // the fence blocks the watermark advance, so no later contract enters the watermark-bounded set
        Postgres.query(sql"""
          select count(*) from pqs_relational.__rel_contracts
          where archived_tx_ix is null and created_tx_ix <= (select tx_ix from pqs_relational.__rel_watermark)
        """) `returns` table(1)
    },
    funcTest("a superseded writer commits neither batch rows nor coverage") {
      val alice = Party("Alice")
      Given:
        DamlSdk.dar(note) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      And:
        DamlSdk.runScript("Note:setup", alice.id)
      And:
        ingest("Genesis")
      And:
        Postgres.query(
          sql"update pqs_relational.__rel_watermark set instance_id = 'another-instance'".update.returns(1)
        )
      And:
        Postgres
          .query {
            sql"""
              create function override_rel_instance_id()
              returns trigger
              language plpgsql
              as $$$$
              begin
                  new.instance_id := 'another-instance';
                  return new;
              end;
              $$$$;

              create trigger trg_override_rel_instance_id
              before update on pqs_relational.__rel_watermark
              for each row
              execute function override_rel_instance_id();
             """.update
          }
          .returns(0)
      And:
        DamlSdk.runScript("Note:setup", alice.id)
      And:
        Pqs.attemptRelationalPipeline(
          "--pipeline-datasource=TransactionTreeStream",
          "--pipeline-ledger-start=Oldest",
          "--pipeline-ledger-stop=Latest",
          s"--pipeline-filter-contracts=$templateFilter"
        )
      And:
        Pqs.exitCode.is(ExitCode.failure)
      And:
        Pqs.stderr.is(stringContaining("PQS writer instance has changed"))
      Expect:
        Postgres.query(sql"""
          select 'contracts' as k, count(*)::text as v from pqs_relational.__rel_contracts
          union all select 'coverage_rows', count(*)::text from pqs_relational.__query_coverage
          order by k
        """) `returns` table {
          "key"           | "value"
          ---             | ---
          "contracts"     | "1"
          "coverage_rows" | "1"
        }
    },
    funcTest("a crash before coverage is durable recovers the acs seed offset and history boundary") {
      val alice = Party("Alice")
      Given:
        DamlSdk.dar(note) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      And:
        DamlSdk.runScript("Note:setup", alice.id)
      And:
        ingest("Latest")
      When:
        Postgres.call(sql"delete from pqs_relational.__query_coverage")
      And:
        ingest("Latest")
      Then:
        FuncTest.retryUntilTimeout(
          Postgres.query(sql"""
            select 'coverage_rows' as k, count(*)::text as v from pqs_relational.__query_coverage
            union all select 'seed_offset_recovered',
              (acs_seed_offset is not null)::text from pqs_relational.__query_coverage
            union all select 'acs_history_lower_bound',
              coalesce(bool_and(history_lower_bound), false)::text
              from pqs_relational.__rel_contracts where source_kind = 'acs_seed'
            order by k
          """) `returns` table {
            "key"                     | "value"
            ---                       | ---
            "acs_history_lower_bound" | "true"
            "coverage_rows"           | "1"
            "seed_offset_recovered"   | "true"
          }
        )
    }
  )
end RecoverySpec
