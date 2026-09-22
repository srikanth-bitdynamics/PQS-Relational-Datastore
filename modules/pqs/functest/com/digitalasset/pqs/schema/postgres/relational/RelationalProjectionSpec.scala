package com.digitalasset.pqs.schema.postgres.relational

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.postgres.relational.projection.{ProjectionApply, ProjectionDefinition, ProjectionRegistry}
import com.digitalasset.pqs.services.daml.{DamlSdk, DamlSource, Party}
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.Pqs
import com.digitalasset.transcode.schema.*
import zio.jdbc.*
import zio.test.*

import scala.language.{implicitConversions, postfixOps}

object RelationalProjectionSpec extends SharedLedgerAndPostgresTest:
  private val note = DamlSource(
    "Note" -> """module Note where
                |
                |template Note
                |  with
                |    owner : Party
                |  where
                |    signatory owner
                |""".stripMargin
  )

  private def schemaFor(pkg: String, module: String, entity: String, fields: Seq[(String, Descriptor)]): Schema =
    val i =
      Identifier(PackageId("pkg"), PackageName(pkg), PackageVersion("1.0.0"), ModuleName(module), EntityName(entity))
    Dictionary.make(
      Template(i, Descriptor.constructor(i, Descriptor.record(fields)), None, false, Seq.empty, Seq.empty)
    )

  private val amountColumnScale =
    sql"""select c.numeric_scale
          from information_schema.columns c
          join __rel_entity e on e.base_table = c.table_name
          where c.table_schema = 'pqs_relational'
            and e.entity_name = 'Note' and e.kind = 'template' and c.column_name = 'amount'"""
      .query[Int]
      .selectOne

  def spec = suite("relational projection apply spec")(
    funcTest("adds typed columns, is idempotent, rejects a type change, and fails closed on an unknown template") {
      val alice = Party("Alice")
      Given:
        DamlSdk.dar(note) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      And:
        Pqs.runRelationalPipeline(
          "--pipeline-datasource=TransactionTreeStream",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          "--pipeline-filter-contracts=*"
        )
      Then:
        for
          setup <- Postgres.query(
            for
              _ <- sql"set search_path to pqs_relational".execute
              entity <-
                sql"select package_name, module_name, entity_name from __rel_entity where entity_name = 'Note' and kind = 'template'"
                  .query[(String, String, String)]
                  .selectOne
              (pkg, module, name) = entity.get
              ownerSchema         = schemaFor(pkg, module, name, Seq("owner" -> Descriptor.party))
              ownerConfig         = Map("asset" -> ProjectionDefinition(Seq(s"$pkg:$module:$name"), Seq("owner")))
              first  <- ProjectionApply.apply(ownerConfig, ownerSchema)
              second <- ProjectionApply.apply(ownerConfig, ownerSchema)
              column <- sql"""select c.data_type, c.is_nullable
                              from information_schema.columns c
                              join __rel_entity e on e.base_table = c.table_name
                              where c.table_schema = 'pqs_relational'
                                and e.entity_name = 'Note' and e.kind = 'template' and c.column_name = 'owner'"""
                .query[(String, String)]
                .selectOne
              drafts <- sql"select count(*) from __query_projection".query[Long].selectOne
            yield (
              assertTrue(
                first match
                  case ProjectionApply.Outcome.Applied(1L, 1, _) => true
                  case _                                         => false
                ,
                second match
                  case ProjectionApply.Outcome.AlreadyApplied(1L, _) => true
                  case _                                             => false
                ,
                column == Some(("text", "YES")),
                drafts == Some(1L)
              ),
              (pkg, module, name)
            )
          )
          (ownerResult, ids)  = setup
          (pkg, module, name) = ids
          amountConfig        = Map("asset" -> ProjectionDefinition(Seq(s"$pkg:$module:$name"), Seq("amount")))
          _ <- Postgres.query(
            for
              _ <- sql"set search_path to pqs_relational".execute
              _ <- ProjectionApply
                .apply(amountConfig, schemaFor(pkg, module, name, Seq("amount" -> Descriptor.numeric(10))))
            yield ()
          )
          conflict <- Postgres
            .query(
              for
                _ <- sql"set search_path to pqs_relational".execute
                o <- ProjectionApply
                  .apply(amountConfig, schemaFor(pkg, module, name, Seq("amount" -> Descriptor.numeric(12))))
              yield o
            )
            .exit
          rollback <- Postgres.query(
            for
              _     <- sql"set search_path to pqs_relational".execute
              scale <- amountColumnScale
              rejected <- ProjectionRegistry.getByHash(
                ProjectionApply
                  .plan(amountConfig, schemaFor(pkg, module, name, Seq("amount" -> Descriptor.numeric(12))))
                  .hash
              )
            yield assertTrue(scale == Some(10), rejected.isEmpty)
          )
          draftsBefore <- Postgres.query(
            sql"set search_path to pqs_relational".execute *>
              sql"select count(*) from __query_projection".query[Long].selectOne
          )
          failClosed <- Postgres
            .query(
              sql"set search_path to pqs_relational".execute *>
                ProjectionApply.apply(
                  Map("typo" -> ProjectionDefinition(Seq(s"$pkg:$module:Ghost"), Seq("owner"))),
                  schemaFor(pkg, module, name, Seq("owner" -> Descriptor.party))
                )
            )
            .exit
          draftsAfter <- Postgres.query(
            sql"set search_path to pqs_relational".execute *>
              sql"select count(*) from __query_projection".query[Long].selectOne
          )
        yield ownerResult && assertTrue(
          conflict.isFailure,
          failClosed.isFailure,
          draftsBefore == draftsAfter
        ) && rollback
    },
    funcTest("rejects a promoted column whose name exceeds the 63-byte identifier limit") {
      val alice = Party("Alice")
      Given:
        DamlSdk.dar(note) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      And:
        Pqs.runRelationalPipeline(
          "--pipeline-datasource=TransactionTreeStream",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          "--pipeline-filter-contracts=*"
        )
      Then:
        for
          ids <- Postgres.query(
            sql"select package_name, module_name, entity_name from pqs_relational.__rel_entity where entity_name = 'Note' and kind = 'template'"
              .query[(String, String, String)]
              .selectOne
          )
          (pkg, module, name) = ids.get
          longName            = "a" * 64
          longConfig          = Map("asset" -> ProjectionDefinition(Seq(s"$pkg:$module:$name"), Seq(longName)))
          longSchema          = schemaFor(pkg, module, name, Seq(longName -> Descriptor.party))
          rejected <- Postgres
            .query(
              for
                _ <- sql"set search_path to pqs_relational".execute
                o <- ProjectionApply.apply(longConfig, longSchema)
              yield o
            )
            .exit
          persisted <- Postgres.query(
            for
              _ <- sql"set search_path to pqs_relational".execute
              d <- ProjectionRegistry.getByHash(ProjectionApply.plan(longConfig, longSchema).hash)
            yield d
          )
        yield assertTrue(rejected.isFailure, persisted.isEmpty)
    }
  )
end RelationalProjectionSpec
