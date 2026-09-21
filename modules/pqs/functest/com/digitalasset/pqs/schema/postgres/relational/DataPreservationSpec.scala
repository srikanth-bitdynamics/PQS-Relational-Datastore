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

object DataPreservationSpec extends SharedLedgerAndPostgresTest:
  private val preserve = DamlSource(
    "Preserve" -> """module Preserve where
                    |
                    |import Daml.Script
                    |import DA.Time (time)
                    |import DA.Date (date, Month(Jan))
                    |
                    |data Color = Red | Green | Blue
                    |  deriving (Eq, Show)
                    |
                    |data Shape
                    |    = ShapeSquare Int
                    |    | ShapeCircle Decimal
                    |  deriving (Eq, Show)
                    |
                    |data Inner = Inner
                    |  with
                    |    a : Int
                    |    b : Text
                    |  deriving (Eq, Show)
                    |
                    |template Everything
                    |  with
                    |    owner : Party
                    |    count : Int
                    |    big : Int
                    |    amount : Numeric 10
                    |    label : Text
                    |    at : Time
                    |    present : Optional Text
                    |    absent : Optional Text
                    |    items : [Int]
                    |    inner : Inner
                    |    shape : Shape
                    |    color : Color
                    |  where
                    |    signatory owner
                    |
                    |setup : Party -> Script ()
                    |setup owner = do
                    |  _ <- submit owner $ createCmd Everything with
                    |        owner
                    |        count = 42
                    |        big = 9223372036854775807
                    |        amount = 12345.0123456789
                    |        label = "quote:\" backslash:\\ newline:\n tab:\t end"
                    |        at = time (date 2026 Jan 2) 3 4 5
                    |        present = Some "here"
                    |        absent = None
                    |        items = [10, 20, 30]
                    |        inner = Inner with a = 7, b = "deep"
                    |        shape = ShapeSquare 9
                    |        color = Blue
                    |  pure ()
                    |""".stripMargin
  )

  private val alice          = Party("Alice")
  private val templateFilter = s"(${preserve.name}:Preserve:Everything)"

  def spec = suite("relational data-preservation spec")(
    funcTest("preserves Daml values of every kind in the create payload and records provenance"):
      Given:
        DamlSdk.dar(preserve) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      And:
        DamlSdk.runScript("Preserve:setup", alice.id)
      And:
        Pqs.runRelationalPipeline(
          "--pipeline-datasource=TransactionTreeStream",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          s"--pipeline-filter-contracts=$templateFilter"
        )
      Then:
        FuncTest.retryUntilTimeout(
          (for
            tbl <- Postgres.query(
              sql"select base_table from pqs_relational.__rel_entity where entity_name = 'Everything' and kind = 'template'"
                .query[String]
                .selectOne
            )
            probes = Syntax(tbl.getOrElse("__missing"))
            result <- Postgres.query(sql"""
              with p as (select payload_json j from pqs_relational.$probes)
              select 'int64' as k, (select j ->> 'count' from p) as v
              union all select 'int64_max', (select j ->> 'big' from p)
              union all select 'numeric', (select j ->> 'amount' from p)
              union all select 'timestamp', (select j ->> 'at' from p)
              union all select 'enum', (select j ->> 'color' from p)
              union all select 'optional_some', (select j ->> 'present' from p)
              union all select 'optional_none', (select jsonb_typeof(j -> 'absent') from p)
              union all select 'list_len', (select jsonb_array_length(j -> 'items')::text from p)
              union all select 'list_head', (select j -> 'items' ->> 0 from p)
              union all select 'nested_a', (select j -> 'inner' ->> 'a' from p)
              union all select 'nested_b', (select j -> 'inner' ->> 'b' from p)
              union all select 'variant_tag', (select j -> 'shape' ->> 'tag' from p)
              union all select 'variant_value', (select j -> 'shape' ->> 'value' from p)
              union all select 'escaping', (select j ->> 'label' from p)
              union all select 'contract_synchronizer',
                (select case when creation_synchronizer_id is not null then 'present' else 'absent' end
                   from pqs_relational.__rel_contracts limit 1)
              union all select 'tx_synchronizer',
                case when exists(
                  select 1 from pqs_relational.__rel_transactions where synchronizer_id is not null and tx_ix > 0
                ) then 'present' else 'absent' end
              order by k
            """)
          yield result) `returns` table {
            "key"                   | "value"
            ---                     | ---
            "contract_synchronizer" | "present"
            "enum"                  | "Blue"
            "escaping"              | "quote:\" backslash:\\ newline:\n tab:\t end"
            "int64"                 | "42"
            "int64_max"             | "9223372036854775807"
            "list_head"             | "10"
            "list_len"              | "3"
            "nested_a"              | "7"
            "nested_b"              | "deep"
            "numeric"               | "12345.0123456789"
            "optional_none"         | "null"
            "optional_some"         | "here"
            "timestamp"             | "2026-01-02T03:04:05Z"
            "tx_synchronizer"       | "present"
            "variant_tag"           | "ShapeSquare"
            "variant_value"         | "9"
          }
        )
  )
end DataPreservationSpec
