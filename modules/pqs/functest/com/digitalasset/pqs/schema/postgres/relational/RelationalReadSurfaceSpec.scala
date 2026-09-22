package com.digitalasset.pqs.schema.postgres.relational

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.services.daml.{DamlSdk, DamlSource, Party}
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.Pqs
import zio.jdbc.*
import zio.test.*

import scala.language.{implicitConversions, postfixOps}

object RelationalReadSurfaceSpec extends SharedLedgerAndPostgresTest:
  private val note = DamlSource(
    "Note" -> """module Note where
                |
                |import Daml.Script
                |
                |template Note
                |  with
                |    owner : Party
                |  where
                |    signatory owner
                |
                |setup : Party -> Script ()
                |setup owner = do
                |  _ <- submit owner $ createCmd Note with owner
                |  pure ()
                |""".stripMargin
  )

  def spec = suite("relational read surface spec")(
    funcTest("the transactions view hides rows committed past the published watermark") {
      val alice = Party("Alice")
      Given:
        DamlSdk.dar(note) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      And:
        DamlSdk.runScript("Note:setup", alice.id)
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
            _         <- sql"set search_path to pqs_relational".execute
            watermark <- sql"select ledger_offset from latest_checkpoint()".query[Long].selectOne.map(_.getOrElse(0L))
            future = watermark + 1000
            _ <- sql"insert into __rel_transactions (tx_ix, ledger_offset) values (999999, $future)".update
            rawMax <- sql"select max(ledger_offset) from __rel_transactions"
              .query[Option[Long]]
              .selectOne
              .map(_.flatten)
            viewMax   <- sql"select max(ledger_offset) from transactions".query[Option[Long]].selectOne.map(_.flatten)
            rawCount  <- sql"select count(*) from __rel_transactions".query[Long].selectOne.map(_.getOrElse(0L))
            viewCount <- sql"select count(*) from transactions".query[Long].selectOne.map(_.getOrElse(0L))
          yield assertTrue(
            rawMax == Some(future),
            viewMax == Some(watermark),
            rawCount == viewCount + 1
          )
        )
    }
  )
end RelationalReadSurfaceSpec
