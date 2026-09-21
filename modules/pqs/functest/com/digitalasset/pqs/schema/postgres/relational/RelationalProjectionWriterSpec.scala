package com.digitalasset.pqs.schema.postgres.relational

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.postgres.relational.projection.{ProjectionApply, ProjectionDefinition}
import com.digitalasset.pqs.services.daml.{DamlSdk, DamlSource, Party}
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.Pqs
import com.digitalasset.transcode.schema.*
import zio.jdbc.*
import zio.test.*

import scala.language.{implicitConversions, postfixOps}

object RelationalProjectionWriterSpec extends SharedLedgerAndPostgresTest:
  private val note = DamlSource(
    "Note" -> """module Note where
                |
                |import Daml.Script
                |
                |template Note
                |  with
                |    owner : Party
                |    noteBody : Text
                |  where
                |    signatory owner
                |
                |setup : Party -> Script ()
                |setup owner = do
                |  _ <- submit owner $ createCmd Note with owner, noteBody = "hello"
                |  pure ()
                |""".stripMargin
  )

  private def schemaFor(pkg: String, module: String, entity: String, fields: Seq[(String, Descriptor)]): Schema =
    val i =
      Identifier(PackageId("pkg"), PackageName(pkg), PackageVersion("1.0.0"), ModuleName(module), EntityName(entity))
    Dictionary.make(
      Template(i, Descriptor.constructor(i, Descriptor.record(fields)), None, false, Seq.empty, Seq.empty)
    )

  private def ingest(start: String) =
    Pqs.runRelationalPipeline(
      "--pipeline-datasource=TransactionTreeStream",
      s"--pipeline-ledger-start=$start",
      "--pipeline-ledger-stop=Latest",
      "--pipeline-filter-contracts=*"
    )

  def spec = suite("relational projection writer spec")(
    funcTest(
      "fills typed columns for contracts ingested under an active projection, leaving pre-activation rows null"
    ) {
      val alice = Party("Alice")
      Given:
        DamlSdk.dar(note) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      And:
        DamlSdk.runScript("Note:setup", alice.id)
      And:
        ingest("Genesis")
      When:
        Postgres.query(
          for
            _ <- sql"set search_path to pqs_relational".execute
            entity <-
              sql"select package_name, module_name, entity_name from __rel_entity where entity_name = 'Note' and kind = 'template'"
                .query[(String, String, String)]
                .selectOne
            (pkg, module, name) = entity.get
            config = Map("asset" -> ProjectionDefinition(Seq(s"$pkg:$module:$name"), Seq("owner", "noteBody")))
            _ <- ProjectionApply.apply(
              config,
              schemaFor(pkg, module, name, Seq("owner" -> Descriptor.party, "noteBody" -> Descriptor.text))
            )
            _ <-
              sql"update __query_projection set status = 'active', activated_at = now() where projection_version = 1".update
          yield ()
        )
      And:
        DamlSdk.runScript("Note:setup", alice.id)
      And:
        ingest("Oldest")
      Then:
        Postgres.query(
          for
            _ <- sql"set search_path to pqs_relational".execute
            base <- sql"select base_table from __rel_entity where entity_name = 'Note' and kind = 'template'"
              .query[String]
              .selectOne
            rows <- SqlFragment(s"""select owner, "noteBody" from ${base.get} order by contract_pk""")
              .query[(Option[String], Option[String])]
              .selectAll
          yield rows match
            case Seq((preOwner, preNote), (postOwner, postNote)) =>
              assertTrue(
                preOwner.isEmpty,
                preNote.isEmpty,
                postOwner.exists(_.startsWith("Alice")),
                postNote.contains("hello")
              )
            case _ => assertTrue(false)
        )
    }
  )
end RelationalProjectionWriterSpec
