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

object InterfaceChoiceSpec extends SharedLedgerAndPostgresTest:
  private val bells = DamlSource(
    "Bells" -> """module Bells where
                 |
                 |interface IBell
                 |  where
                 |    viewtype BellView
                 |
                 |    nonconsuming choice Ring : Text
                 |      controller (view this).owner
                 |      do pure "dong"
                 |
                 |data BellView = BellView with owner : Party
                 |  deriving (Eq, Ord, Show)
                 |""".stripMargin
  )

  private val chimes = DamlSource(
    "Chimes" -> """module Chimes where
                  |
                  |import Daml.Script
                  |import DA.Functor (void)
                  |import Bells
                  |
                  |template Handbell
                  |  with
                  |    owner : Party
                  |  where
                  |    signatory owner
                  |    interface instance IBell for Handbell where
                  |      view = BellView with owner
                  |
                  |setup : Party -> Script ()
                  |setup owner = void do
                  |  cid <- submit owner $ createCmd (Handbell with owner)
                  |  submit owner $ exerciseCmd (toInterfaceContractId @IBell cid) Ring
                  |""".stripMargin
  ).dependsOn(bells)

  def spec = suite("relational interface-choice spec")(
    funcTest("registers an interface-defined choice and resolves an exercise made through the interface") {
      val alice = Party("Alice")
      Given:
        DamlSdk.dar(chimes) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      And:
        DamlSdk.runScript("Chimes:setup", alice.id)
      And:
        Pqs.runRelationalPipeline(
          "--pipeline-datasource=TransactionTreeStream",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          "--pipeline-filter-contracts=*"
        )
      Then:
        FuncTest.retryUntilTimeout(
          (for result <- Postgres.query(sql"""
              select 'choice_registered' as k,
                (select count(*)::text from pqs_relational.__rel_choice c
                   join pqs_relational.__rel_entity e on c.entity_pk = e.pk
                   where e.entity_name = 'IBell' and e.kind = 'interface' and c.choice = 'Ring') as v
              union all select 'exercise_choice_is_interface',
                (select count(*)::text from pqs_relational.__rel_exercises x
                   join pqs_relational.__rel_choice c on x.choice_entity_pk = c.pk
                   join pqs_relational.__rel_entity e on c.entity_pk = e.pk
                   where x.choice_name = 'Ring' and e.entity_name = 'IBell' and e.kind = 'interface')
              union all select 'exercise_template_is_handbell',
                (select count(*)::text from pqs_relational.__rel_exercises x
                   join pqs_relational.__rel_entity e on x.contract_template_entity_pk = e.pk
                   where x.choice_name = 'Ring' and e.entity_name = 'Handbell' and e.kind = 'template')
              order by k
            """)
          yield result) `returns` table {
            "key"                           | "value"
            ---                             | ---
            "choice_registered"             | "1"
            "exercise_choice_is_interface"  | "1"
            "exercise_template_is_handbell" | "1"
          }
        )
    }
  )
end InterfaceChoiceSpec
