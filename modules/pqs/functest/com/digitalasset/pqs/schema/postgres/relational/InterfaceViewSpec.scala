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

object InterfaceViewSpec extends SharedLedgerAndPostgresTest:
  private val ifaces = DamlSource(
    "Ifaces" -> """module Ifaces where
                  |
                  |interface IAsset where
                  |  viewtype AssetView
                  |
                  |data AssetView = AssetView with owner : Party, label : Text
                  |  deriving (Eq, Ord, Show)
                  |""".stripMargin
  )

  private val tmpls = DamlSource(
    "Tmpls" -> """module Tmpls where
                 |
                 |import Daml.Script
                 |import Ifaces
                 |
                 |template Token
                 |  with
                 |    owner : Party
                 |    label : Text
                 |  where
                 |    signatory owner
                 |    interface instance IAsset for Token where
                 |      view = AssetView with owner = owner, label = label
                 |
                 |setup : Party -> Script ()
                 |setup owner = do
                 |  _ <- submit owner $ createCmd Token with owner, label = "hi"
                 |  pure ()
                 |""".stripMargin
  ).dependsOn(ifaces)

  def spec = suite("relational interface-view spec")(
    funcTest("stores a template's interface view in its relv_ table alongside the base payload") {
      val alice = Party("Alice")
      Given:
        DamlSdk.dar(tmpls) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      And:
        DamlSdk.runScript("Tmpls:setup", alice.id)
      And:
        // a wildcard filter includes every known interface, so the ledger delivers the view alongside the template
        Pqs.runRelationalPipeline(
          "--pipeline-datasource=TransactionTreeStream",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          "--pipeline-filter-contracts=*"
        )
      Then:
        FuncTest.retryUntilTimeout(
          (for
            base <- Postgres.query(
              sql"select base_table from pqs_relational.__rel_entity where entity_name = 'Token' and kind = 'template'"
                .query[String]
                .selectOne
            )
            view <- Postgres.query(
              sql"select base_table from pqs_relational.__rel_entity where entity_name = 'IAsset' and kind = 'interface'"
                .query[String]
                .selectOne
            )
            b = Syntax(base.getOrElse("__nobase"))
            v = Syntax(view.getOrElse("__noview"))
            result <- Postgres.query(sql"""
              select 'implements' as k,
                (select count(*)::text from pqs_relational.__rel_implements i
                   join pqs_relational.__rel_entity t on i.template_pk = t.pk
                   join pqs_relational.__rel_entity f on i.interface_pk = f.pk
                   where t.entity_name = 'Token' and f.entity_name = 'IAsset') as v
              union all select 'base_rows', (select count(*)::text from pqs_relational.$b)
              union all select 'view_rows', (select count(*)::text from pqs_relational.$v)
              union all select 'payload_label', (select payload_json ->> 'label' from pqs_relational.$b)
              union all select 'view_label', (select view_json ->> 'label' from pqs_relational.$v)
              union all select 'view_owner_matches',
                (select case when view_json ->> 'owner' = ${alice.id} then 'true' else 'false' end
                   from pqs_relational.$v)
              union all select 'shared_pk',
                case when (select contract_pk from pqs_relational.$b) = (select contract_pk from pqs_relational.$v)
                  then 'true' else 'false' end
              order by k
            """)
          yield result) `returns` table {
            "key"                | "value"
            ---                  | ---
            "base_rows"          | "1"
            "implements"         | "1"
            "payload_label"      | "hi"
            "shared_pk"          | "true"
            "view_label"         | "hi"
            "view_owner_matches" | "true"
            "view_rows"          | "1"
          }
        )
    },
    funcTest("an interface filter also delivers the implementing template's payload via the schema closure") {
      val alice = Party("Alice")
      Given:
        DamlSdk.dar(tmpls) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      And:
        DamlSdk.runScript("Tmpls:setup", alice.id)
      And:
        // Filtering only by the interface still pulls the implementing template into the subscription, so the ledger
        // delivers the template create argument alongside the view. This is the invariant the by-identity payload
        // routing relies on: a create always carries its template payload, routed to the base table by entity kind.
        Pqs.runRelationalPipeline(
          "--pipeline-datasource=TransactionTreeStream",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          "--pipeline-filter-contracts=Ifaces.IAsset"
        )
      Then:
        FuncTest.retryUntilTimeout(
          (for
            base <- Postgres.query(
              sql"select base_table from pqs_relational.__rel_entity where entity_name = 'Token' and kind = 'template'"
                .query[String]
                .selectOne
            )
            view <- Postgres.query(
              sql"select base_table from pqs_relational.__rel_entity where entity_name = 'IAsset' and kind = 'interface'"
                .query[String]
                .selectOne
            )
            b = Syntax(base.getOrElse("__nobase"))
            v = Syntax(view.getOrElse("__noview"))
            result <- Postgres.query(sql"""
              select 'contract_rows' as k, (select count(*)::text from pqs_relational.__rel_contracts) as v
              union all select 'base_rows', (select count(*)::text from pqs_relational.$b)
              union all select 'view_rows', (select count(*)::text from pqs_relational.$v)
              order by k
            """)
          yield result) `returns` table {
            "key"           | "value"
            ---             | ---
            "base_rows"     | "1"
            "contract_rows" | "1"
            "view_rows"     | "1"
          }
        )
    }
  )
end InterfaceViewSpec
