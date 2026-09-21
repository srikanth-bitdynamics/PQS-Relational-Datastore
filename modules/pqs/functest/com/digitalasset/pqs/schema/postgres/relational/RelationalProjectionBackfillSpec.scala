package com.digitalasset.pqs.schema.postgres.relational

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.postgres.relational.projection.{
  ProjectionApply,
  ProjectionBackfill,
  ProjectionBinding,
  ProjectionDefinition,
  ProjectionRegistry
}
import com.digitalasset.pqs.services.daml.{DamlSdk, DamlSource, Party}
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.Pqs
import com.digitalasset.transcode.codec.json.JsonCodec
import com.digitalasset.transcode.schema.*
import zio.jdbc.*
import zio.test.*

import scala.language.{implicitConversions, postfixOps}

object RelationalProjectionBackfillSpec extends SharedLedgerAndPostgresTest:
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

  private def schemaFor(pkgId: String, pkg: String, version: String, module: String, entity: String): Schema =
    val i =
      Identifier(PackageId(pkgId), PackageName(pkg), PackageVersion(version), ModuleName(module), EntityName(entity))
    Dictionary.make(
      Template(
        i,
        Descriptor.constructor(
          i,
          Descriptor.record(Seq("owner" -> Descriptor.party, "noteBody" -> Descriptor.text))
        ),
        None,
        false,
        Seq.empty,
        Seq.empty
      )
    )

  private def ingest(start: String) =
    Pqs.runRelationalPipeline(
      "--pipeline-datasource=TransactionTreeStream",
      s"--pipeline-ledger-start=$start",
      "--pipeline-ledger-stop=Latest",
      "--pipeline-filter-contracts=*"
    )

  def spec = suite("relational projection backfill spec")(
    funcTest("backfill fills pre-activation rows and the writer fills post-activation rows with no null gap") {
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
            id <-
              sql"""select c.representative_package_id, pkg.name, pkg.version, e.module_name, e.entity_name
                    from __rel_contracts c
                    join __rel_entity e on e.pk = c.template_entity_pk
                    join __rel_package pkg on pkg.id = c.representative_package_id
                    where e.entity_name = 'Note' and e.kind = 'template' limit 1"""
                .query[(String, String, String, String, String)]
                .selectOne
            (pkgId, pkg, version, module, entity) = id.get
            schema                                = schemaFor(pkgId, pkg, version, module, entity)
            codec                                 = DescriptorSchemaProcessor.assertProcess(schema, JsonCodec())
            config = Map("asset" -> ProjectionDefinition(Seq(s"$pkg:$module:$entity"), Seq("owner", "noteBody")))
            _     <- ProjectionApply.apply(config, schema)
            draft <- ProjectionRegistry.latestDraft
            version1 = draft.get
            shape <- ProjectionRegistry.resolvedShapeOf(version1)
            shapes = shape.fold(Map.empty)(ProjectionBinding.parse)
            through <- ProjectionBackfill.run(codec, shapes)
            _       <- ProjectionRegistry.setBackfilledThrough(version1, through)
            _       <- ProjectionRegistry.activate(version1)
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
            case Seq((owner1, note1), (owner2, note2)) =>
              assertTrue(
                owner1.exists(_.startsWith("Alice")),
                note1.contains("hello"),
                owner2.exists(_.startsWith("Alice")),
                note2.contains("hello")
              )
            case _ => assertTrue(false)
        )
    }
  )
end RelationalProjectionBackfillSpec
