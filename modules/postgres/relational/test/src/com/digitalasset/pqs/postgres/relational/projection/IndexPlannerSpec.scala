package com.digitalasset.pqs.postgres.relational.projection

import com.digitalasset.pqs.postgres.relational.projection.IndexPlanner.*
import zio.test.*

object IndexPlannerSpec extends ZIOSpecDefault:
  def spec = suite("index planner")(
    test("parseOrder reads a bare column, an explicit asc, and a desc token"):
      assertTrue(
        IndexPlanner.parseOrder("amount") == OrderKey("amount", true),
        IndexPlanner.parseOrder("amount asc") == OrderKey("amount", true),
        IndexPlanner.parseOrder("created_tx_ix desc") == OrderKey("created_tx_ix", false)
      )
    ,
    test("builds one index with sorted equality columns and the declared ordering"):
      val result = IndexPlanner.plan(
        Seq(ProjectionQuery(filter = Seq("owner", "amount"), order = Seq("created_tx_ix desc"))),
        Set("owner", "amount", "created_tx_ix")
      )
      assertTrue(
        result.diagnostics.isEmpty,
        result.indexes == Seq(IndexSpec(Seq("amount", "owner"), Seq(OrderKey("created_tx_ix", false)))),
        result.indexes.map(_.columns) == Seq(Seq("amount", "owner", "created_tx_ix"))
      )
    ,
    test("drops an index whose ordering is a prefix of another with the same equality"):
      val result = IndexPlanner.plan(
        Seq(
          ProjectionQuery(filter = Seq("owner"), order = Seq("created_tx_ix")),
          ProjectionQuery(filter = Seq("owner"), order = Seq("created_tx_ix", "amount desc"))
        ),
        Set("owner", "created_tx_ix", "amount")
      )
      assertTrue(
        result.diagnostics.isEmpty,
        result.indexes == Seq(
          IndexSpec(Seq("owner"), Seq(OrderKey("created_tx_ix", true), OrderKey("amount", false)))
        )
      )
    ,
    test("skips a query that references an unknown column and reports one diagnostic"):
      val result = IndexPlanner.plan(
        Seq(ProjectionQuery(filter = Seq("owner"), order = Seq("nonexistent asc"))),
        Set("owner")
      )
      assertTrue(
        result.indexes.isEmpty,
        result.diagnostics.length == 1,
        result.diagnostics.exists(_.contains("nonexistent"))
      )
    ,
    test("deduplicates queries that yield the same index specification"):
      val query  = ProjectionQuery(filter = Seq("owner", "amount"), order = Seq("created_tx_ix desc"))
      val result = IndexPlanner.plan(Seq(query, query), Set("owner", "amount", "created_tx_ix"))
      assertTrue(
        result.indexes == Seq(IndexSpec(Seq("amount", "owner"), Seq(OrderKey("created_tx_ix", false))))
      )
    ,
    test("ignores an empty query without producing an index or a diagnostic"):
      val result = IndexPlanner.plan(Seq(ProjectionQuery()), Set("owner"))
      assertTrue(result.indexes.isEmpty, result.diagnostics.isEmpty)
  )
end IndexPlannerSpec
