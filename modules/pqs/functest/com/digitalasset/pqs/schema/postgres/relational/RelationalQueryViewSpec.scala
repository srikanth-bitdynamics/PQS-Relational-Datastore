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
import com.digitalasset.transcode.schema.*
import zio.jdbc.*
import zio.test.*

import scala.language.{implicitConversions, postfixOps}

object RelationalQueryViewSpec extends SharedLedgerAndPostgresTest:
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
                |setupAndArchive : Party -> Script ()
                |setupAndArchive owner = do
                |  cid <- submit owner $ createCmd Note with owner, noteBody = "hello"
                |  submit owner $ archiveCmd cid
                |  pure ()
                |""".stripMargin
  )

  private def schemaFor(pkgId: String, pkg: String, version: String, module: String, entity: String): Schema =
    val i =
      Identifier(PackageId(pkgId), PackageName(pkg), PackageVersion(version), ModuleName(module), EntityName(entity))
    Dictionary.make(
      Template(
        i,
        Descriptor.constructor(i, Descriptor.record(Seq("owner" -> Descriptor.party, "noteBody" -> Descriptor.text))),
        None,
        false,
        Seq.empty,
        Seq.empty
      )
    )

  def spec = suite("relational query view spec")(
    funcTest("a q_ view exposes typed columns and honours the historical watermark bounds") {
      val alice = Party("Alice")
      Given:
        DamlSdk.dar(note) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      And:
        DamlSdk.runScript("Note:setupAndArchive", alice.id)
      And:
        Pqs.runRelationalPipeline(
          "--pipeline-datasource=TransactionTreeStream",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          "--pipeline-filter-contracts=*"
        )
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
            (pkgId, pkg, pkgVersion, module, name) = id.get
            schema                                 = schemaFor(pkgId, pkg, pkgVersion, module, name)
            config = Map("asset" -> ProjectionDefinition(Seq(s"$pkg:$module:$name"), Seq("owner", "noteBody")))
            _     <- ProjectionApply.apply(config, schema)
            draft <- ProjectionRegistry.latestDraft
            version = draft.get
            shape <- ProjectionRegistry.resolvedShapeOf(version)
            shapes = shape.fold(Map.empty)(ProjectionBinding.parse)
            through <- ProjectionBackfill.run(schema, shapes, version)
            _       <- ProjectionRegistry.setBackfilledThrough(version, through)
            _       <- ProjectionRegistry.activate(version)
          yield ()
        )
      Then:
        Postgres.query(
          for
            _    <- sql"set search_path to pqs_relational".execute
            view <- RelationalQueries.queryViewOf("Note")
            offsets <-
              sql"select created_at_offset, archived_at_offset from __rel_contracts where archived_at_offset is not null limit 1"
                .query[(Long, Long)]
                .selectOne
            (createdOffset, archivedOffset) = offsets.get
            _ <- sql"select set_latest($createdOffset)".query[Long].selectOne
            activeRows <-
              SqlFragment(s"""select owner, "noteBody", payload_json::text from ${view} order by created_tx_ix""")
                .query[(Option[String], Option[String], Option[String])]
                .selectAll
            _             <- sql"select set_latest($archivedOffset)".query[Long].selectOne
            archivedCount <- SqlFragment(s"select count(*) from ${view}").query[Long].selectOne.map(_.getOrElse(-1L))
          yield activeRows match
            case Seq((owner, body, payload)) =>
              assertTrue(
                owner.exists(_.startsWith("Alice")),
                body.contains("hello"),
                payload.exists(_.contains("hello")),
                archivedCount == 0L
              )
            case _ => assertTrue(false)
        )
    }
  )
end RelationalQueryViewSpec
