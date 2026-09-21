package com.digitalasset.pqs.postgres.relational.projection

import ujson.Value

import java.security.MessageDigest

final case class ProjectionQuery(filter: Seq[String] = Seq.empty, order: Seq[String] = Seq.empty)

final case class ProjectionDefinition(
    templates: Seq[String],
    promote: Seq[String],
    queries: Seq[ProjectionQuery] = Seq.empty
)

object ProjectionDefinition:
  private def arr(items: Seq[String]): Value = ujson.Arr(items.map(ujson.Str(_))*)

  def canonical(d: ProjectionDefinition): ProjectionDefinition =
    ProjectionDefinition(
      d.templates.sorted,
      d.promote.sorted,
      d.queries
        .map(q => ProjectionQuery(q.filter.sorted, q.order))
        .sortBy(q => (q.filter.mkString(","), q.order.mkString(",")))
    )

  def toJson(projections: Map[String, ProjectionDefinition]): Value =
    ujson.Obj.from(projections.toSeq.sortBy(_._1).map { (name, d) =>
      name -> ujson.Obj(
        "templates" -> arr(d.templates),
        "promote"   -> arr(d.promote),
        "queries"   -> ujson.Arr(d.queries.map(q => ujson.Obj("filter" -> arr(q.filter), "order" -> arr(q.order)))*)
      )
    })

  def fromJson(json: Value): Map[String, ProjectionDefinition] =
    json.obj.map { (name, d) =>
      name -> ProjectionDefinition(
        d("templates").arr.map(_.str).toSeq,
        d("promote").arr.map(_.str).toSeq,
        d("queries").arr
          .map(q => ProjectionQuery(q("filter").arr.map(_.str).toSeq, q("order").arr.map(_.str).toSeq))
          .toSeq
      )
    }.toMap

  def canonicalHash(json: Value): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(ujson.write(json).getBytes("UTF-8"))
      .map(b => f"${b & 0xff}%02x")
      .mkString
end ProjectionDefinition
