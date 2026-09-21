package com.digitalasset.pqs.postgres.relational.projection

import com.digitalasset.transcode.schema.*
import zio.test.*

object ProjectionApplySpec extends ZIOSpecDefault:
  private def id(pkg: String, ver: String, entity: String) =
    Identifier(PackageId(pkg), PackageName("Finance"), PackageVersion(ver), ModuleName("Main"), EntityName(entity))

  private def tmpl(i: Identifier, fields: Seq[(String, Descriptor)]): Template[Descriptor] =
    Template(i, Descriptor.constructor(i, Descriptor.record(fields)), None, false, Seq.empty, Seq.empty)

  private def assetSchema(scale: Int) =
    Dictionary.make(
      tmpl(
        id("p1", "1.0.0", "Asset"),
        Seq(
          "owner"  -> Descriptor.party,
          "amount" -> Descriptor.numeric(scale),
          "tags"   -> Descriptor.list(Descriptor.text)
        )
      )
    )

  def spec = suite("projection apply planner")(
    test("plans typed columns for promoted fields and a shape-sensitive hash"):
      val config = Map("asset" -> ProjectionDefinition(Seq("Finance:Main:Asset"), Seq("owner", "amount")))
      val plan   = ProjectionApply.plan(config, assetSchema(10))
      assertTrue(
        plan.columns.map(c => (c.field.name, c.field.pgType.sql, c.field.nullable)) == Seq(
          ("owner", "text", false),
          ("amount", "numeric(38, 10)", false)
        ),
        plan.diagnostics.isEmpty,
        plan.hash == ProjectionApply.plan(config, assetSchema(10)).hash,
        plan.hash != ProjectionApply.plan(config, assetSchema(12)).hash
      )
    ,
    test("emits diagnostics for an unknown template and a non-promotable field"):
      val config = Map(
        "asset"   -> ProjectionDefinition(Seq("Finance:Main:Asset"), Seq("owner", "tags")),
        "missing" -> ProjectionDefinition(Seq("Finance:Main:Ghost"), Seq("owner"))
      )
      val plan = ProjectionApply.plan(config, assetSchema(10))
      assertTrue(
        plan.columns.map(_.field.name) == Seq("owner"),
        plan.diagnostics.exists(_.contains("'tags'")),
        plan.diagnostics.exists(_.contains("Finance:Main:Ghost"))
      )
  )
end ProjectionApplySpec
