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
            config = Map("asset" -> ProjectionDefinition(Seq(s"$pkg:$module:$entity"), Seq("owner", "noteBody")))
            _     <- ProjectionApply.apply(config, schema)
            draft <- ProjectionRegistry.latestDraft
            version1 = draft.get
            shape <- ProjectionRegistry.resolvedShapeOf(version1)
            shapes = shape.fold(Map.empty)(ProjectionBinding.parse)
            through <- ProjectionBackfill.run(schema, shapes, version1)
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
            _    <- sql"set search_path to pqs_relational".execute
            base <- RelationalQueries.baseTableOf("Note")
            rows <- SqlFragment(s"""select owner, "noteBody" from ${base} order by contract_pk""")
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
    },
    funcTest("backfill chunks incrementally and resumes from the stored cursor") {
      val alice = Party("Alice")
      Given:
        DamlSdk.dar(note) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      And:
        DamlSdk.runScript("Note:setup", alice.id)
      And:
        DamlSdk.runScript("Note:setup", alice.id)
      And:
        DamlSdk.runScript("Note:setup", alice.id)
      And:
        ingest("Genesis")
      Then:
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
            (pkgId, pkg, pkgVersion, module, entity) = id.get
            schema                                   = schemaFor(pkgId, pkg, pkgVersion, module, entity)
            config = Map("asset" -> ProjectionDefinition(Seq(s"$pkg:$module:$entity"), Seq("owner", "noteBody")))
            _     <- ProjectionApply.apply(config, schema)
            draft <- ProjectionRegistry.latestDraft
            versionNo = draft.get
            shape <- ProjectionRegistry.resolvedShapeOf(versionNo)
            shapes = shape.fold(Map.empty)(ProjectionBinding.parse)
            base <- RelationalQueries.baseTableOf("Note")
            ordered <- SqlFragment(
              s"""select c.created_tx_ix, p.contract_pk from ${base} p
                  join __rel_contracts c on c.contract_pk = p.contract_pk
                  order by c.created_tx_ix, p.contract_pk"""
            ).query[(Long, Long)].selectAll
            (firstTx, firstPk) = ordered.headOption.getOrElse((-1L, 0L))
            through <- sql"select tx_ix from latest_checkpoint()".query[Long].selectOne.map(_.getOrElse(0L))
            qualified = s"$pkg:$module:$entity"
            _ <- sql"""insert into __rel_backfill_progress
                         (projection_version, qualified, cursor_tx_ix, cursor_pk, through_ix, completed)
                       values ($versionNo, $qualified, $firstTx, $firstPk, $through, false)""".update
            _ <- ProjectionBackfill.run(schema, shapes, versionNo, 1)
            rows <- SqlFragment(
              s"""select p.owner, p."noteBody" from ${base} p
                  join __rel_contracts c on c.contract_pk = p.contract_pk
                  order by c.created_tx_ix, p.contract_pk"""
            ).query[(Option[String], Option[String])].selectAll
            done <-
              sql"select completed from __rel_backfill_progress where projection_version = $versionNo and qualified = $qualified"
                .query[Boolean]
                .selectOne
            encoding <- sql"select numeric_as_string, int64_as_string, exclude_nulls from __rel_encoding"
              .query[(Boolean, Boolean, Boolean)]
              .selectOne
            samePin     <- sql"call __rel_pin_encoding(true, true, false)".execute.exit
            conflictPin <- sql"call __rel_pin_encoding(false, true, false)".execute.exit
          yield rows match
            case Seq((owner1, note1), (owner2, note2), (owner3, note3)) =>
              assertTrue(
                owner1.isEmpty,
                note1.isEmpty,
                owner2.exists(_.startsWith("Alice")),
                note2.contains("hello"),
                owner3.exists(_.startsWith("Alice")),
                note3.contains("hello"),
                done.contains(true),
                encoding.contains((true, true, false)),
                samePin.isSuccess,
                conflictPin.isFailure
              )
            case _ => assertTrue(false)
        )
    },
    funcTest("rerunning backfill catches up to a later watermark and types rows created after the first target") {
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
            config = Map("asset" -> ProjectionDefinition(Seq(s"$pkg:$module:$entity"), Seq("owner", "noteBody")))
            _     <- ProjectionApply.apply(config, schema)
            draft <- ProjectionRegistry.latestDraft
            version1 = draft.get
            shape <- ProjectionRegistry.resolvedShapeOf(version1)
            shapes = shape.fold(Map.empty)(ProjectionBinding.parse)
            _ <- ProjectionBackfill.run(schema, shapes, version1)
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
            draft <- ProjectionRegistry.latestDraft
            versionNo = draft.get
            shape <- ProjectionRegistry.resolvedShapeOf(versionNo)
            shapes = shape.fold(Map.empty)(ProjectionBinding.parse)
            _    <- ProjectionBackfill.run(schema, shapes, versionNo)
            base <- RelationalQueries.baseTableOf("Note")
            rows <- SqlFragment(
              s"""select p.owner, p."noteBody" from ${base} p
                  join __rel_contracts c on c.contract_pk = p.contract_pk
                  order by c.created_tx_ix, p.contract_pk"""
            ).query[(Option[String], Option[String])].selectAll
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
    },
    funcTest("rerunning a completed backfill at an unchanged watermark reopens the lineage and types trailing rows") {
      val alice = Party("Alice")
      Given:
        DamlSdk.dar(note) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      And:
        DamlSdk.runScript("Note:setup", alice.id)
      And:
        DamlSdk.runScript("Note:setup", alice.id)
      And:
        ingest("Genesis")
      Then:
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
            config = Map("asset" -> ProjectionDefinition(Seq(s"$pkg:$module:$entity"), Seq("owner", "noteBody")))
            _     <- ProjectionApply.apply(config, schema)
            draft <- ProjectionRegistry.latestDraft
            versionNo = draft.get
            shape <- ProjectionRegistry.resolvedShapeOf(versionNo)
            shapes = shape.fold(Map.empty)(ProjectionBinding.parse)
            base <- RelationalQueries.baseTableOf("Note")
            ordered <- SqlFragment(
              s"""select c.created_tx_ix, p.contract_pk from ${base} p
                  join __rel_contracts c on c.contract_pk = p.contract_pk
                  order by c.created_tx_ix, p.contract_pk"""
            ).query[(Long, Long)].selectAll
            (firstTx, firstPk) = ordered.headOption.getOrElse((-1L, 0L))
            through <- sql"select tx_ix from latest_checkpoint()".query[Long].selectOne.map(_.getOrElse(0L))
            qualified = s"$pkg:$module:$entity"
            _ <- sql"""insert into __rel_backfill_progress
                         (projection_version, qualified, cursor_tx_ix, cursor_pk, through_ix, completed)
                       values ($versionNo, $qualified, $firstTx, $firstPk, $through, true)""".update
            _ <- ProjectionBackfill.run(schema, shapes, versionNo)
            rows <- SqlFragment(
              s"""select p.owner, p."noteBody" from ${base} p
                  join __rel_contracts c on c.contract_pk = p.contract_pk
                  order by c.created_tx_ix, p.contract_pk"""
            ).query[(Option[String], Option[String])].selectAll
            done <-
              sql"select completed from __rel_backfill_progress where projection_version = $versionNo and qualified = $qualified"
                .query[Boolean]
                .selectOne
          yield rows match
            case Seq((owner1, note1), (owner2, note2)) =>
              assertTrue(
                owner1.isEmpty,
                note1.isEmpty,
                owner2.exists(_.startsWith("Alice")),
                note2.contains("hello"),
                done.contains(true)
              )
            case _ => assertTrue(false)
        )
    }
  )
end RelationalProjectionBackfillSpec
