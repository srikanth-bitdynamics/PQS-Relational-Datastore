package com.digitalasset.pqs.postgres.relational

import com.digitalasset.pqs.postgres.relational.projection.ProjectionRegistry
import zio.jdbc.*
import zio.{Scope, ZEnvironment, ZIO}

object WriterFence:
  private val key = ProjectionRegistry.writerLockKey

  def acquire(pool: ZConnectionPool, maxConnections: Int): ZIO[Scope, Throwable, ZEnvironment[ZConnection]] =
    for
      _ <- ZIO
        .fail(
          new RuntimeException(
            "relational ingest needs at least two datastore connections so the writer-liveness lock does not starve the pool"
          )
        )
        .when(maxConnections < 2)
      connEnv <- pool.transaction.build
      _       <- sql"set local lock_timeout = '30s'".execute.provideEnvironment(connEnv)
      _       <- sql"select 1 from pg_advisory_lock($key)".query[Int].selectOne.provideEnvironment(connEnv)
      _       <- connEnv.get[ZConnection].access(_.commit())
      _ <- ZIO.addFinalizer(
        sql"select case when pg_advisory_unlock($key) then 1 else 0 end"
          .query[Int]
          .selectOne
          .provideEnvironment(connEnv)
          .ignoreLogged
      )
    yield connEnv

  def requireIdle[R, A](pool: ZConnectionPool, maxConnections: Int)(body: ZIO[R, Throwable, A]): ZIO[R, Throwable, A] =
    ZIO.scoped {
      for
        _ <- ZIO
          .fail(
            new RuntimeException(
              "relational schema apply needs at least two datastore connections so the writer-liveness lock does not starve the migration"
            )
          )
          .when(maxConnections < 2)
        connEnv <- pool.transaction.build
        acquired <- sql"select case when pg_try_advisory_lock($key) then 1 else 0 end"
          .query[Int]
          .selectOne
          .map(_.getOrElse(0))
          .provideEnvironment(connEnv)
        _ <- acquired.compareTo(1) match
          case 0 => connEnv.get[ZConnection].access(_.commit())
          case _ =>
            ZIO.fail(
              new RuntimeException(
                "cannot apply schema: a relational ingest writer is live; stop the writer, then re-run schema apply"
              )
            )
        _ <- ZIO.addFinalizer(
          sql"select case when pg_advisory_unlock($key) then 1 else 0 end"
            .query[Int]
            .selectOne
            .provideEnvironment(connEnv)
            .ignoreLogged
        )
        result <- body
      yield result
    }
end WriterFence
