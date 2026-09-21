package com.digitalasset.pqs.schema.postgres.relational

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.{DamlSdk, DamlSource, Party}
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.Pqs
import zio.jdbc.*
import zio.jdbc.SqlFragment.Segment.Syntax
import zio.test.*

import scala.language.{implicitConversions, postfixOps}

object SchemaSpec extends SharedLedgerAndPostgresTest:
  private val pingPong = DamlSource(
    "PingPong" -> """module PingPong where
                    |
                    |import Daml.Script
                    |import DA.Foldable (forA_)
                    |
                    |template Ping
                    |  with
                    |    owner : Party
                    |  where
                    |    signatory owner
                    |
                    |    choice Stop : ()
                    |      controller owner
                    |      do pure ()
                    |
                    |setup : Party -> Script ()
                    |setup owner = do
                    |  _ <- submit owner $ createCmd Ping with owner
                    |  pure ()
                    |
                    |consume : Party -> Script ()
                    |consume owner = do
                    |  pings <- query @Ping owner
                    |  forA_ pings $ \(cid, _) -> submit owner $ exerciseCmd cid Stop
                    |""".stripMargin
  )

  private val alice          = Party("Alice")
  private val templateFilter = s"(${pingPong.name}:PingPong:Ping)"

  def spec = suite("relational schema spec")(
    funcTest("ingests a create, drains a consuming archive, and records coverage"):
      Given:
        DamlSdk.dar(pingPong) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      And:
        DamlSdk.runScript("PingPong:setup", alice.id)
      And:
        Pqs.runRelationalPipeline(
          "--pipeline-datasource=TransactionTreeStream",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          s"--pipeline-filter-contracts=$templateFilter"
        )
      Then:
        FuncTest.retryUntilTimeout(
          Postgres.query(sql"""
            select 'active_contracts' as k, count(*)::text as v
              from pqs_relational.__rel_contracts where archived_tx_ix is null
            union all select 'contracts', count(*)::text from pqs_relational.__rel_contracts
            union all select 'coverage', count(*)::text from pqs_relational.__query_coverage
            union all select 'create_events', count(*)::text
              from pqs_relational.__query_events where event_kind = 'create'
            union all select 'signatory_vis', count(*)::text
              from pqs_relational.__rel_contract_visibility where party = ${alice.id} and role = 'signatory'
            order by k
          """) `returns` table {
            "key"              | "count"
            ---                | ---
            "active_contracts" | "1"
            "contracts"        | "1"
            "coverage"         | "1"
            "create_events"    | "1"
            "signatory_vis"    | "1"
          }
        )
      And:
        DamlSdk.runScript("PingPong:consume", alice.id)
      And:
        Pqs.runRelationalPipeline(
          "--pipeline-datasource=TransactionTreeStream",
          "--pipeline-ledger-start=Oldest",
          "--pipeline-ledger-stop=Latest",
          s"--pipeline-filter-contracts=$templateFilter"
        )
      Then:
        FuncTest.retryUntilTimeout(
          Postgres.query(sql"""
            select 'archived' as k, count(*)::text as v
              from pqs_relational.__rel_contracts where archived_tx_ix is not null
            union all select 'consuming_exercise', count(*)::text
              from pqs_relational.__query_events where event_kind = 'exercise' and archive_source = 'consuming_exercise'
            order by k
          """) `returns` table {
            "key"                | "count"
            ---                  | ---
            "archived"           | "1"
            "consuming_exercise" | "1"
          }
        )
      And:
        // the create payload lands as jsonb in the per-template base table
        FuncTest.retryUntilTimeout(
          for
            tbl <- Postgres.query(
              sql"select base_table from pqs_relational.__rel_entity where entity_name = 'Ping' and kind = 'template'"
                .query[String]
                .selectOne
            )
            cnt <- Postgres.query(
              sql"""select count(*)::text from pqs_relational.${Syntax(tbl.getOrElse("__missing"))}
                    where payload_json ->> 'owner' = ${alice.id}""".query[String].selectOne
            )
          yield assertTrue(tbl.isDefined, cnt.contains("1"))
        )
      When:
        // deleting the contract cascades to its typed payload row (on delete cascade)
        Postgres.call(sql"delete from pqs_relational.__rel_contracts")
      Then:
        FuncTest.retryUntilTimeout(
          for
            tbl <- Postgres.query(
              sql"select base_table from pqs_relational.__rel_entity where entity_name = 'Ping' and kind = 'template'"
                .query[String]
                .selectOne
            )
            cnt <- Postgres.query(
              sql"select count(*)::text from pqs_relational.${Syntax(tbl.getOrElse("__missing"))}"
                .query[String]
                .selectOne
            )
          yield assertTrue(cnt.contains("0"))
        )
  )
end SchemaSpec
