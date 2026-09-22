package com.digitalasset.pqs.schema.postgres.relational

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.postgres.relational.projection.{ProjectionDefinition, ProjectionRegistry}
import com.digitalasset.pqs.services.daml.{DamlSdk, DamlSource, Party}
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.Pqs
import zio.jdbc.*
import zio.test.*

import scala.language.{implicitConversions, postfixOps}

object ProjectionRegistrySpec extends SharedLedgerAndPostgresTest:
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

  def spec = suite("relational projection registry spec")(
    funcTest("inserts a draft projection version and reads it back through the registry") {
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
        val projections = Map("asset" -> ProjectionDefinition(Seq("Note:Note:Note"), Seq("owner")))
        val definition  = ProjectionDefinition.toJson(projections)
        Postgres.query(
          for
            _       <- sql"set search_path to pqs_relational".execute
            version <- ProjectionRegistry.insertDraft(definition, "hash-1", ujson.Obj(), 1)
            rows    <- ProjectionRegistry.list
            one     <- ProjectionRegistry.get(version)
          yield assertTrue(
            version == 1L,
            rows.map(r => (r.version, r.status, r.hash)) == Seq((1L, "draft", "hash-1")),
            one.map(r => ProjectionDefinition.fromJson(r.definition)) == Some(projections)
          )
        )
    },
    funcTest("reinitialising a package id with a different name or version is rejected") {
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
        def initialize(name: String, version: String, id: String) =
          Postgres.query(
            sql"set search_path to pqs_relational".execute *>
              sql"call __rel_initialize_package($name, $version, $id)".execute.either
          )
        for
          pkg <- Postgres.query(
            sql"set search_path to pqs_relational".execute *>
              sql"select id, name, version from __rel_package limit 1".query[(String, String, String)].selectOne
          )
          (id, name, version) = pkg.getOrElse(("", "", ""))
          same         <- initialize(name, version, id)
          wrongName    <- initialize("WrongName", version, id)
          wrongVersion <- initialize(name, "99.99.99", id)
        yield assertTrue(
          pkg.isDefined,
          same.isRight,
          wrongName.isLeft,
          wrongVersion.isLeft
        )
    }
  )
end ProjectionRegistrySpec
