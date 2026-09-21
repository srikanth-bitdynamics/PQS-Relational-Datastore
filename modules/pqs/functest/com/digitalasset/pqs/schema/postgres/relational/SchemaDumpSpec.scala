package com.digitalasset.pqs.schema.postgres.relational

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.services.daml.DamlSdk.onlyPostgresVersion
import com.digitalasset.pqs.services.daml.{DamlSdk, DamlSource, Party}
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.Pqs
import zio.ZIO
import zio.test.*

import scala.language.implicitConversions

/** Regenerate the checked-in dump with the `REGENERATE_SCHEMA_DUMP` env var; the failure message prints the command. */
object SchemaDumpSpec extends SharedLedgerAndPostgresTest:
  private val asset = DamlSource(
    "Asset" -> """module Asset where
                 |
                 |import Daml.Script
                 |
                 |template Asset
                 |  with
                 |    owner : Party
                 |  where
                 |    signatory owner
                 |
                 |noop : Party -> Script ()
                 |noop _ = pure ()
                 |""".stripMargin
  )

  private val alice         = Party("Alice")
  private val workspaceRoot = sys.env.get("MILL_WORKSPACE_ROOT").fold(os.pwd)(os.Path(_))
  private val target =
    workspaceRoot / "modules" / "postgres" / "relational" / "resources" / "db" / "schema-dump.sql"
  private val regenerate = sys.env.get("REGENERATE_SCHEMA_DUMP").flatMap(_.toBooleanOption).exists(identity)

  private val staleMessage =
    s"The reference SQL dump ($target) is stale: regenerate it with `REGENERATE_SCHEMA_DUMP=true mill pqs.functest.testOnly " +
      s"${getClass.getName}` and commit it"

  private val copyrightHeaderRegex = """-- Copyright \(c\) \d{4} [^\n]*\n-- SPDX-License-Identifier: [^\n]*\n\n""".r

  def spec = suite("RelationalSchemaDumpSpec")(
    funcTest(s"$target matches the schema produced by the current relational Flyway migrations"):
      Given:
        DamlSdk.dar(asset) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      When:
        Pqs.runRelationalPipeline(
          "--pipeline-datasource=TransactionTreeStream",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          s"--pipeline-filter-contracts=(${asset.name}:Asset:Asset)"
        )
      Then:
        if regenerate then Postgres.dumpSchemaTo(target).as(assertCompletes)
        else
          for
            current   <- Postgres.dumpSchema
            exists    <- ZIO.attemptBlocking(os.exists(target))
            checkedIn <- ZIO.attemptBlocking(if exists then os.read(target) else "")
          yield assertTrue(exists) && (assert(current)(
            Assertion.equalTo(copyrightHeaderRegex.replaceFirstIn(checkedIn, ""))
          ) ?? staleMessage)
  ) @@ onlyPostgresVersion(">=17.0 <18.0.0")
end SchemaDumpSpec
