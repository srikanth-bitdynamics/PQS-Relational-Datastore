package com.digitalasset.pqs.schema.postgres.relational

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.postgres.relational.WriterFence
import com.digitalasset.pqs.postgres.relational.projection.ProjectionRegistry
import com.digitalasset.pqs.services.daml.{DamlSdk, DamlSource, Party}
import com.digitalasset.pqs.services.postgres.{Database, Postgres}
import com.digitalasset.pqs.services.pqs.Pqs
import zio.ZIO
import zio.jdbc.*
import zio.test.*

import scala.language.{implicitConversions, postfixOps}

object WriterLivenessFenceSpec extends SharedLedgerAndPostgresTest:
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

  private def status(version: Long) =
    Postgres.query(
      sql"set search_path to pqs_relational".execute *>
        sql"select status::text from __query_projection where projection_version = $version".query[String].selectOne
    )

  private def activate(version: Long) =
    Postgres
      .query(sql"set search_path to pqs_relational".execute *> ProjectionRegistry.activate(version))
      .exit

  private def currentInstance =
    Postgres.query(sql"select instance_id from pqs_relational.__rel_watermark".query[String].selectOne)

  def spec = suite("relational writer liveness fence spec")(
    funcTest("activation is refused while a writer holds the liveness lock and succeeds once it is released") {
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
          version <- Postgres.query(
            sql"set search_path to pqs_relational".execute *> (
              for
                v         <- ProjectionRegistry.insertDraft(ujson.Obj(), "hash-live", ujson.Obj(), 1)
                watermark <- sql"select tx_ix from latest_checkpoint()".query[Long].selectOne.map(_.getOrElse(0L))
                _         <- ProjectionRegistry.setBackfilledThrough(v, watermark)
              yield v
            )
          )
          held <- ZIO.serviceWithZIO[Database] { db =>
            ZIO.scoped {
              for
                holder <- db.transaction.build
                _ <- sql"select 1 from pg_advisory_lock(${ProjectionRegistry.writerLockKey})"
                  .query[Int]
                  .selectOne
                  .provideEnvironment(holder)
                refused         <- activate(version)
                statusWhileLive <- status(version)
                _ <- sql"select pg_advisory_unlock(${ProjectionRegistry.writerLockKey})"
                  .query[Boolean]
                  .selectOne
                  .provideEnvironment(holder)
              yield (refused, statusWhileLive)
            }
          }
          (refused, statusWhileLive) = held
          released    <- activate(version)
          statusAfter <- status(version)
        yield assertTrue(
          refused.isFailure,
          statusWhileLive.contains("draft"),
          released.isSuccess,
          statusAfter.contains("active")
        )
    },
    funcTest("schema apply is refused while a writer holds the liveness lock, leaving the instance id unchanged") {
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
          before <- currentInstance
          held <- ZIO.serviceWithZIO[Database] { db =>
            ZIO.scoped {
              for
                holder <- db.transaction.build
                _ <- sql"select 1 from pg_advisory_lock(${ProjectionRegistry.writerLockKey})"
                  .query[Int]
                  .selectOne
                  .provideEnvironment(holder)
                refused <- WriterFence
                  .requireIdle(db.connectionPool, 2)(
                    Postgres.query(sql"update pqs_relational.__rel_watermark set instance_id = 'intruder'".update)
                  )
                  .exit
                during <- currentInstance
                _ <- sql"select pg_advisory_unlock(${ProjectionRegistry.writerLockKey})"
                  .query[Boolean]
                  .selectOne
                  .provideEnvironment(holder)
              yield (refused, during)
            }
          }
          (refused, during) = held
        yield assertTrue(refused.isFailure, before.isDefined, during == before)
    }
  )
end WriterLivenessFenceSpec
