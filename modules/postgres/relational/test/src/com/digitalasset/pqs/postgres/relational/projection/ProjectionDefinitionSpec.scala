package com.digitalasset.pqs.postgres.relational.projection

import zio.test.*

object ProjectionDefinitionSpec extends ZIOSpecDefault:
  private val projections = Map(
    "asset" -> ProjectionDefinition(
      templates = Seq("MyPackage:Main:Asset"),
      promote = Seq("owner", "currency", "amount"),
      queries = Seq(ProjectionQuery(filter = Seq("owner", "currency"), order = Seq("created_tx_ix desc")))
    )
  )

  def spec = suite("projection definition")(
    test("round-trips through JSON"):
      assertTrue(ProjectionDefinition.fromJson(ProjectionDefinition.toJson(projections)) == projections)
    ,
    test("hash is stable and independent of projection insertion order"):
      val a = ProjectionDefinition.toJson(projections ++ Map("b" -> ProjectionDefinition(Seq("P:M:B"), Seq("x"))))
      val b = ProjectionDefinition.toJson(Map("b" -> ProjectionDefinition(Seq("P:M:B"), Seq("x"))) ++ projections)
      assertTrue(
        ProjectionDefinition.canonicalHash(a) == ProjectionDefinition.canonicalHash(b),
        ProjectionDefinition.canonicalHash(a) != ProjectionDefinition.canonicalHash(
          ProjectionDefinition.toJson(projections)
        )
      )
    ,
    test("canonical form ignores set ordering but honours query order"):
      def hash(d: ProjectionDefinition) =
        ProjectionDefinition.canonicalHash(ProjectionDefinition.toJson(Map("p" -> ProjectionDefinition.canonical(d))))
      val base = ProjectionDefinition(
        Seq("P:M:B", "P:M:A"),
        Seq("owner", "amount"),
        Seq(ProjectionQuery(Seq("b", "a"), Seq("amount asc", "owner desc")))
      )
      val setReordered = ProjectionDefinition(
        Seq("P:M:A", "P:M:B"),
        Seq("amount", "owner"),
        Seq(ProjectionQuery(Seq("a", "b"), Seq("amount asc", "owner desc")))
      )
      val orderReordered = ProjectionDefinition(
        Seq("P:M:B", "P:M:A"),
        Seq("owner", "amount"),
        Seq(ProjectionQuery(Seq("b", "a"), Seq("owner desc", "amount asc")))
      )
      assertTrue(hash(base) == hash(setReordered), hash(base) != hash(orderReordered))
  )
end ProjectionDefinitionSpec
