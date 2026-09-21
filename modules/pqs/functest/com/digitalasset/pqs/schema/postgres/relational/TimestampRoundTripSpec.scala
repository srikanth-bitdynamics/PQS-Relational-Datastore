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

object TimestampRoundTripSpec extends SharedLedgerAndPostgresTest:
  private val clock = DamlSource(
    "Clock" -> """module Clock where
                 |
                 |import Daml.Script
                 |import DA.Time (time, addRelTime, microseconds)
                 |import DA.Date (date, Month(Jan))
                 |
                 |template Clock
                 |  with
                 |    owner : Party
                 |    observedAt : Time
                 |  where
                 |    signatory owner
                 |
                 |setup : Party -> Script ()
                 |setup owner = do
                 |  _ <- submit owner $ createCmd Clock with
                 |    owner
                 |    observedAt = addRelTime (time (date 2026 Jan 2) 3 4 5) (microseconds 123456)
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

  def spec = suite("relational timestamp round-trip spec")(
    funcTest("preserves microsecond precision writing a Daml Time into a typed timestamptz column") {
      val alice = Party("Alice")
      Given:
        DamlSdk.dar(clock) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      And:
        DamlSdk.runScript("Clock:setup", alice.id)
      And:
        ingest("Genesis")
      When:
        Postgres.query(
          for
            _ <- sql"set search_path to pqs_relational".execute
            entity <-
              sql"select package_name, module_name, entity_name from __rel_entity where entity_name = 'Clock' and kind = 'template'"
                .query[(String, String, String)]
                .selectOne
            (pkg, module, name) = entity.get
            config              = Map("asset" -> ProjectionDefinition(Seq(s"$pkg:$module:$name"), Seq("observedAt")))
            _ <- ProjectionApply.apply(
              config,
              schemaFor(pkg, module, name, Seq("owner" -> Descriptor.party, "observedAt" -> Descriptor.timestamp))
            )
            _ <-
              sql"update __query_projection set status = 'active', activated_at = now() where projection_version = 1".update
          yield ()
        )
      And:
        DamlSdk.runScript("Clock:setup", alice.id)
      And:
        ingest("Oldest")
      Then:
        Postgres.query(
          for
            _ <- sql"set search_path to pqs_relational".execute
            base <- sql"select base_table from __rel_entity where entity_name = 'Clock' and kind = 'template'"
              .query[String]
              .selectOne
            rows <- SqlFragment(s"""select "observedAt"::text from ${base.get} order by contract_pk""")
              .query[Option[String]]
              .selectAll
          yield rows match
            case Seq(pre, post) =>
              assertTrue(pre.isEmpty, post.exists(_.contains(".123456")))
            case _ => assertTrue(false)
        )
    }
  )
end TimestampRoundTripSpec
