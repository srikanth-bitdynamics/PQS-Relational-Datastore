package com.digitalasset.pqs.postgres.relational.projection

import zio.test.*

object ProjectionBindingSpec extends ZIOSpecDefault:
  private val json =
    """{"asset":{"Finance:Main:Asset":[
         {"name":"owner","type":"text","nullable":false,"position":0},
         {"name":"amount","type":"numeric(38, 10)","nullable":false,"position":1},
         {"name":"active","type":"boolean","nullable":false,"position":2},
         {"name":"status","type":"text","nullable":false,"position":3,"enum":["A","B"]},
         {"name":"when","type":"timestamptz","nullable":true,"position":4},
         {"name":"born","type":"date","nullable":true,"position":6}
       ]}}"""

  def spec = suite("projection binding")(
    test("reconstructs the resolved shape from stored JSON with pg types and union field count"):
      val shape = ProjectionBinding.parse(json)("Finance:Main:Asset")
      assertTrue(
        shape.lineage == Shape.Lineage("Finance", "Main", "Asset", Shape.EntityKind.Template),
        shape.unionFieldCount == 7,
        shape.promoted.map(f => (f.name, f.pgType.sql, f.nullable, f.position)) == Seq(
          ("owner", "text", false, 0),
          ("amount", "numeric(38, 10)", false, 1),
          ("active", "boolean", false, 2),
          ("status", "text", false, 3),
          ("when", "timestamptz", true, 4),
          ("born", "date", true, 6)
        ),
        shape.promoted.flatMap(_.enumCases) == Seq(Seq("A", "B"))
      )
    ,
    test("an empty document binds no shapes"):
      assertTrue(ProjectionBinding.parse("{}").isEmpty)
  )
end ProjectionBindingSpec
