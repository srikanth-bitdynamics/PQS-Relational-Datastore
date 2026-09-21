package com.digitalasset.pqs.schema.postgres.relational

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.services.daml.{DamlSdk, DamlSource, Party}
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.Pqs
import zio.jdbc.*
import zio.test.*

import scala.language.{implicitConversions, postfixOps}

object RelationalMultiPackageSpec extends SharedLedgerAndPostgresTest:
  private val note = DamlSource(
    "Note" -> """module Note where
                |
                |template Note
                |  with
                |    owner : Party
                |  where
                |    signatory owner
                |""".stripMargin
  )

  def spec = suite("relational multi package spec")(
    funcTest(
      "the base table name is per template lineage, so same-named templates from different packages do not collide"
    ) {
      val alice = Party("Alice")
      Given:
        DamlSdk.dar(note) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      And:
        Pqs.runRelationalPipeline(
          "--pipeline-datasource=TransactionTreeStream",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          "--pipeline-filter-contracts=*"
        )
      Then:
        Postgres.query(
          for
            _ <- sql"set search_path to pqs_relational".execute
            a <- sql"select __rel_typed_table_name('finance', 'Main', 'Asset', 'template'::rel_entity_kind)"
              .query[String]
              .selectOne
            b <- sql"select __rel_typed_table_name('trading', 'Main', 'Asset', 'template'::rel_entity_kind)"
              .query[String]
              .selectOne
            again <- sql"select __rel_typed_table_name('finance', 'Main', 'Asset', 'template'::rel_entity_kind)"
              .query[String]
              .selectOne
          yield assertTrue(
            a.exists(_.startsWith("rel_")),
            b.exists(_.startsWith("rel_")),
            a != b,
            a == again
          )
        )
    }
  )
end RelationalMultiPackageSpec
