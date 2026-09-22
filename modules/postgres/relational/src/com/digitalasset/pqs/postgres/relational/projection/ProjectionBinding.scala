package com.digitalasset.pqs.postgres.relational.projection

import ujson.Value
import zio.ZIO
import zio.jdbc.*

object ProjectionBinding:
  def activeShapes: ZIO[ZConnection, Throwable, Map[String, Shape.ResolvedShape]] =
    sql"""select resolved_shape::text from __query_projection
          where status = 'active' order by projection_version desc limit 1"""
      .query[String]
      .selectOne
      .map(_.fold(Map.empty)(parse))

  def parse(json: String): Map[String, Shape.ResolvedShape] =
    ujson
      .read(json)
      .obj
      .values
      .flatMap(_.obj)
      .map((qualified, cols) => qualified -> cols.arr.map(fieldOf).toSeq)
      .groupMapReduce(_._1)(_._2)(_ ++ _)
      .map((qualified, fields) => qualified -> toShape(qualified, reconcile(qualified, fields)))

  private def reconcile(qualified: String, fields: Seq[Shape.PromotedField]): Seq[Shape.PromotedField] =
    fields.groupBy(_.position).toSeq.sortBy(_._1).map { (position, group) =>
      group.distinctBy(f => (f.name, f.pgType.sql, f.nullable, f.enumCases)) match
        case Seq(field) => field
        case _ =>
          throw new RuntimeException(
            s"projection binding for $qualified has conflicting definitions at position $position"
          )
    }

  private def fieldOf(v: Value): Shape.PromotedField =
    Shape.PromotedField(
      v("name").str,
      pgType(v("type").str),
      v("nullable").bool,
      v("position").num.toInt,
      v.obj.get("enum").map(_.arr.map(_.str).toSeq)
    )

  private def toShape(qualified: String, fields: Seq[Shape.PromotedField]): Shape.ResolvedShape =
    val parts           = qualified.split(":")
    val lineage         = Shape.Lineage(parts(0), parts(1), parts(2), Shape.EntityKind.Template)
    val unionFieldCount = fields.map(_.position).maxOption.map(_ + 1).getOrElse(0)
    Shape.ResolvedShape(lineage, unionFieldCount, fields, Seq.empty, Seq.empty)

  private def pgType(sql: String): Shape.PgType =
    sql match
      case "bigint"      => Shape.PgType.Bigint
      case "boolean"     => Shape.PgType.Bool
      case "text"        => Shape.PgType.Text
      case "date"        => Shape.PgType.Date
      case "timestamptz" => Shape.PgType.Timestamptz
      case other         => Shape.PgType.Numeric(numericScale(other))

  private def numericScale(sql: String): Int =
    sql.substring(sql.indexOf(',') + 1, sql.indexOf(')')).trim.toInt
end ProjectionBinding
