package com.digitalasset.pqs.schema.postgres.relational

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.{DamlSdk, DamlSource, Party}
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.Pqs
import zio.jdbc.*
import zio.test.*

import scala.language.{implicitConversions, postfixOps}

object SchemaGenerationSpec extends SharedLedgerAndPostgresTest:
  private val plain = DamlSource(
    "Plain" -> """module Plain where
                 |
                 |template Plain
                 |  with
                 |    owner : Party
                 |  where
                 |    signatory owner
                 |""".stripMargin
  )

  def spec = suite("relational schema generation spec")(
    funcTest("identities that differ only by case or kind resolve to distinct physical tables") {
      val alice = Party("Alice")
      Given:
        DamlSdk.dar(plain) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      And:
        Pqs.runRelationalPipeline(
          "--pipeline-datasource=TransactionTreeStream",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          "--pipeline-filter-contracts=*"
        )
      Then:
        Postgres.query(sql"""
          select 'case_distinct' as k,
            (pqs_relational.__rel_typed_table_name('review', 'Names', 'Token', 'template')
               <> pqs_relational.__rel_typed_table_name('REVIEW', 'Names', 'Token', 'template'))::text as v
          union all select 'kind_distinct',
            (pqs_relational.__rel_typed_table_name('acme', 'Mod', 'Thing', 'template')
               <> pqs_relational.__rel_typed_table_name('acme', 'Mod', 'Thing', 'interface'))::text
          order by k
        """) `returns` table {
          "key"           | "value"
          ---             | ---
          "case_distinct" | "true"
          "kind_distinct" | "true"
        }
    }
  )
end SchemaGenerationSpec
