package com.digitalasset.pqs.postgres.relational.projection

import com.digitalasset.transcode.schema.*
import zio.test.*

object ShapeResolverSpec extends ZIOSpecDefault:
  private def id(pkg: String, ver: String, entity: String) =
    Identifier(PackageId(pkg), PackageName("Finance"), PackageVersion(ver), ModuleName("Main"), EntityName(entity))

  private def tmpl(
      i: Identifier,
      fields: Seq[(String, Descriptor)],
      isInterface: Boolean = false
  ): Template[Descriptor] =
    Template(i, Descriptor.constructor(i, Descriptor.record(fields)), None, isInterface, Seq.empty, Seq.empty)

  private val statusEnum =
    Descriptor.constructor(id("e1", "1.0.0", "Status"), Descriptor.enumeration(Seq("Active", "Frozen")))

  def spec = suite("shape resolver")(
    test("promotes primitives + enum, marks optionals nullable, keeps structural fields JSON, unions across versions"):
      val fields = Seq(
        "owner"  -> Descriptor.party,
        "amount" -> Descriptor.numeric(10),
        "active" -> Descriptor.bool,
        "status" -> statusEnum,
        "tags"   -> Descriptor.list(Descriptor.text),
        "memo"   -> Descriptor.optional(Descriptor.text)
      )
      val v1 = tmpl(id("p1", "1.0.0", "Asset"), fields)
      val v2 = tmpl(id("p2", "2.0.0", "Asset"), fields :+ ("issuer" -> Descriptor.optional(Descriptor.text)))
      val asset =
        Shape.resolveAll(Dictionary.make(v1, v2))(Shape.Lineage("Finance", "Main", "Asset", Shape.EntityKind.Template))
      assertTrue(
        asset.unionFieldCount == 7,
        asset.jsonOnly == Seq("tags"),
        asset.promoted.map(f => (f.name, f.pgType.sql, f.nullable, f.position)) == Seq(
          ("owner", "text", false, 0),
          ("amount", "numeric(38, 10)", false, 1),
          ("active", "boolean", false, 2),
          ("status", "text", false, 3),
          ("memo", "text", true, 5),
          ("issuer", "text", true, 6)
        ),
        asset.promoted.find(_.name == "status").flatMap(_.enumCases) == Some(Seq("Active", "Frozen"))
      )
    ,
    test("refuses a field whose type diverges across versions and keeps it JSON with a diagnostic"):
      val v1 = tmpl(id("p1", "1.0.0", "Rec"), Seq("x" -> Descriptor.int64))
      val v2 = tmpl(id("p2", "2.0.0", "Rec"), Seq("x" -> Descriptor.text))
      val shape =
        Shape.resolveAll(Dictionary.make(v1, v2))(Shape.Lineage("Finance", "Main", "Rec", Shape.EntityKind.Template))
      assertTrue(
        shape.promoted.isEmpty,
        shape.jsonOnly == Seq("x"),
        shape.diagnostics.exists(_.contains("diverges"))
      )
    ,
    test("an interface entity resolves under the Interface kind"):
      val iface = tmpl(id("i1", "1.0.0", "IAsset"), Seq("owner" -> Descriptor.party), isInterface = true)
      val shape =
        Shape.resolveAll(Dictionary.make(iface))(Shape.Lineage("Finance", "Main", "IAsset", Shape.EntityKind.Interface))
      assertTrue(shape.promoted.map(_.name) == Seq("owner"))
    ,
    test("an enum that appends a case across versions stays promoted with the unioned case list"):
      def status(cases: Seq[String]) =
        Descriptor.constructor(id("e1", "1.0.0", "Status"), Descriptor.enumeration(cases))
      val v1 = tmpl(id("p1", "1.0.0", "Order"), Seq("status" -> status(Seq("Open", "Closed"))))
      val v2 = tmpl(id("p2", "2.0.0", "Order"), Seq("status" -> status(Seq("Open", "Closed", "Paused"))))
      val shape =
        Shape.resolveAll(Dictionary.make(v1, v2))(Shape.Lineage("Finance", "Main", "Order", Shape.EntityKind.Template))
      assertTrue(
        shape.jsonOnly.isEmpty,
        shape.diagnostics.isEmpty,
        shape.promoted.map(f => (f.name, f.pgType.sql, f.nullable)) == Seq(("status", "text", false)),
        shape.promoted.head.enumCases == Some(Seq("Open", "Closed", "Paused"))
      )
    ,
    test("an enum whose cases are reordered across versions diverges and stays JSON"):
      def status(cases: Seq[String]) =
        Descriptor.constructor(id("e1", "1.0.0", "Status"), Descriptor.enumeration(cases))
      val v1 = tmpl(id("p1", "1.0.0", "Order"), Seq("status" -> status(Seq("Open", "Closed"))))
      val v2 = tmpl(id("p2", "2.0.0", "Order"), Seq("status" -> status(Seq("Closed", "Open"))))
      val shape =
        Shape.resolveAll(Dictionary.make(v1, v2))(Shape.Lineage("Finance", "Main", "Order", Shape.EntityKind.Template))
      assertTrue(
        shape.promoted.isEmpty,
        shape.jsonOnly == Seq("status"),
        shape.diagnostics.exists(_.contains("diverges"))
      )
  )
end ShapeResolverSpec
