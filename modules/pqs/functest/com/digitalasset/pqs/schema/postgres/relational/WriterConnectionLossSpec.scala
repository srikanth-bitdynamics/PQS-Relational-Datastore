// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.schema.postgres.relational

import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.postgres.backend.transact
import com.digitalasset.pqs.postgres.relational.WriterFence
import com.digitalasset.pqs.postgres.relational.projection.ProjectionRegistry
import com.digitalasset.pqs.services.postgres.{Postgres, ProductionPool}
import zio.{Promise, Schedule, ZIO, durationInt}
import zio.jdbc.*
import zio.test.*

object WriterConnectionLossSpec extends FuncTest[Postgres]:
  def shared = Postgres.instance

  def spec = suite("writer connection loss")(
    funcTest("activation drains an in-flight batch after fence loss and rejects later stale batches") {
      Given:
        Postgres.database >+> ProductionPool.layer(maxConnections = 5)
      Then:
        ZIO.scoped {
          for
            _ <- ProductionPool.relationalSchema
            version <- transact {
              for
                v <- ProjectionRegistry.insertDraft(ujson.Obj(), "draft", "shape-draft", ujson.Obj(), 1)
                _ <- ProjectionRegistry.setBackfilledThrough(v, 0L)
              yield v
            }
            pool          <- ZIO.service[ZConnectionPool]
            fence         <- WriterFence.acquire(pool, 5)
            fenceIdentity <- WriterFence.identity.provideEnvironment(fence)
            pid = fenceIdentity.pid
            _ <- fence.get[ZConnection].access(_.commit())
            wrongGeneration <- transact(
              WriterFence.check(fenceIdentity.copy(backendStart = "1970-01-01T00:00:00Z"))
            ).either
            started <- Promise.make[Nothing, Unit]
            finish  <- Promise.make[Nothing, Unit]
            batch <- transact {
              WriterFence.check(fenceIdentity) *>
                sql"insert into __rel_transactions (tx_ix, ledger_offset) values (1, 100)".update *>
                started.succeed(()) *> finish.await
            }.forkScoped
            _ <- started.await
            _ <- transact(sql"select pg_terminate_backend($pid)".query[Boolean].selectOne)
            _ <- transact(sql"""select not exists (select 1 from pg_locks where locktype = 'advisory'
                       and classid = ${ProjectionRegistry.writerLockKey >>> 32}::oid
                       and objid = ${ProjectionRegistry.writerLockKey & 0xffffffffL}::oid
                       and objsubid = 1 and pid = $pid and mode = 'ExclusiveLock')""".query[Boolean].selectOne)
              .repeat(Schedule.spaced(20.millis) && Schedule.recurUntil[Option[Boolean]](_.contains(true)))
              .timeoutFail(new RuntimeException("fence writer lock not released after backend termination"))(10.seconds)
            activation <- transact(ProjectionRegistry.activate(version)).forkScoped
            waiting <- transact(sql"""select exists (select 1 from pg_locks where locktype = 'advisory'
                         and classid = ${WriterFence.activityLockKey >>> 32}::oid
                         and objid = ${WriterFence.activityLockKey & 0xffffffffL}::oid
                         and mode = 'ExclusiveLock' and not granted)""".query[Boolean].selectOne)
              .repeat(Schedule.spaced(20.millis) && Schedule.recurUntil[Option[Boolean]](_.contains(true)))
              .timeoutFail(new RuntimeException("activation did not wait for the in-flight writer"))(10.seconds)
            _ <- finish.succeed(())
            _ <- batch.join *> activation.join
            stale <- transact(
              WriterFence.check(fenceIdentity) *>
                sql"insert into __rel_transactions (tx_ix, ledger_offset) values (2, 200)".update
            ).either
            rows   <- transact(sql"select count(*) from __rel_transactions".query[Long].selectOne)
            active <- transact(ProjectionRegistry.get(version))
          yield assertTrue(
            waiting._2.contains(true),
            wrongGeneration.isLeft,
            stale.isLeft,
            rows.contains(1L),
            active.exists(_.status == "active")
          )
        }
    }
  )
