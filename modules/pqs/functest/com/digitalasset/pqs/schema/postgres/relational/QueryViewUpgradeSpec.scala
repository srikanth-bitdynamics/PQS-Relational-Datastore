// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.schema.postgres.relational

import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.postgres.backend.transact
import com.digitalasset.pqs.postgres.relational.projection.{ProjectionRegistry, QueryViews, Shape}
import com.digitalasset.pqs.services.postgres.{Postgres, ProductionPool}
import zio.jdbc.*
import zio.test.*

object QueryViewUpgradeSpec extends FuncTest[Postgres]:
  def shared = Postgres.instance

  private val lineage = Shape.Lineage("Test", "Main", "Asset", Shape.EntityKind.Template)
  private val owner   = Shape.PromotedField("owner", Shape.PgType.Text, false, 0, None)
  private val amount  = Shape.PromotedField("amount", Shape.PgType.Bigint, false, 1, None)
  private def shapes(fields: Shape.PromotedField*) = Map(
    lineage.qualified -> Shape.ResolvedShape(lineage, 2, fields.toSeq, Seq.empty, Seq.empty)
  )

  private val setup = ProductionPool.relationalSchema *> transact {
    for
      _ <- sql"create table payload (contract_pk bigint, payload_json jsonb, owner text, amount bigint)".execute
      _ <- sql"""insert into __rel_entity (package_name, module_name, entity_name, kind, base_table, query_view)
                 values ('Test', 'Main', 'Asset', 'template', 'payload', 'q_asset')""".update
      _ <- QueryViews.publish(shapes(owner))
      _ <- sql"grant select on q_asset to public".execute
      _ <- sql"grant select (owner) on q_asset to public".execute
      _ <- sql"create view consumer as select owner from q_asset".execute
      _ <- sql"comment on view q_asset is 'public contract'".execute
    yield ()
  }

  private val identity = transact(
    sql"""select c.oid::bigint, c.relacl::text, a.attacl::text, obj_description(c.oid, 'pg_class')
           from pg_class c join pg_attribute a on a.attrelid = c.oid and a.attname = 'owner'
           where c.oid = 'q_asset'::regclass"""
      .query[(Long, String, String, String)]
      .selectOne
  )

  def spec = suite("query view upgrades")(
    funcTest("republishing and adding columns preserves identity, grants, comments and dependent views") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _         <- setup
          before    <- identity
          _         <- transact(QueryViews.publish(shapes(owner, amount)))
          _         <- transact(QueryViews.publish(shapes(amount, owner)))
          after     <- identity
          dependent <- transact(sql"select count(*) from consumer".query[Long].selectOne)
          columns <- transact(sql"""select attname from pg_attribute where attrelid = 'q_asset'::regclass
                                    and attnum > 0 order by attnum""".query[String].selectAll)
        yield assertTrue(
          before.isDefined,
          before == after,
          dependent.contains(0L),
          columns.takeRight(2).toSeq == Seq("payload_json", "amount")
        )
    },
    funcTest("removing a published field or template is refused without damaging consumers") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _         <- setup *> transact(QueryViews.publish(shapes(owner, amount)))
          before    <- identity
          removed   <- transact(QueryViews.publish(shapes(owner))).either
          absent    <- transact(QueryViews.publish(Map.empty)).either
          after     <- identity
          dependent <- transact(sql"select count(*) from consumer".query[Long].selectOne)
        yield assertTrue(removed.isLeft, absent.isLeft, before == after, dependent.contains(0L))
    },
    funcTest("a rejected view upgrade rolls back the active projection change") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _ <- setup
          draft <- transact {
            for
              old  <- ProjectionRegistry.insertDraft(ujson.Obj(), "old", "shape-empty", ujson.Obj(), 1)
              _    <- sql"update __query_projection set status = 'active' where projection_version = $old".update
              next <- ProjectionRegistry.insertDraft(ujson.Obj(), "next", "shape-empty", ujson.Obj(), 1)
              _    <- ProjectionRegistry.setBackfilledThrough(next, 0L)
            yield next
          }
          result <- transact(ProjectionRegistry.activate(draft)).either
          statuses <- transact(
            sql"select status::text from __query_projection order by projection_version".query[String].selectAll
          )
        yield assertTrue(result.isLeft, statuses.toSeq == Seq("active", "draft"))
    }
  )
