package com.digitalasset.pqs.postgres.relational.projection

import com.digitalasset.transcode.schema.*

object Shape:
  enum EntityKind:
    case Template, Interface

  final case class Lineage(packageName: String, moduleName: String, entityName: String, kind: EntityKind):
    def qualified: String = s"$packageName:$moduleName:$entityName"

  sealed trait PgType:
    def sql: String
  object PgType:
    case object Bigint extends PgType:
      val sql = "bigint"
    final case class Numeric(scale: Int) extends PgType:
      val sql = s"numeric(38, $scale)"
    case object Date extends PgType:
      val sql = "date"
    case object Timestamptz extends PgType:
      val sql = "timestamptz"
    case object Text extends PgType:
      val sql = "text"
    case object Bool extends PgType:
      val sql = "boolean"

  final case class PromotedField(
      name: String,
      pgType: PgType,
      nullable: Boolean,
      position: Int,
      enumCases: Option[Seq[String]]
  )

  final case class ResolvedShape(
      lineage: Lineage,
      unionFieldCount: Int,
      promoted: Seq[PromotedField],
      jsonOnly: Seq[String],
      diagnostics: Seq[String]
  )

  def resolveAll(schema: Dictionary[Descriptor]): Map[Lineage, ResolvedShape] =
    schema.entities
      .groupBy(t => lineageOf(t.templateId, t.isInterface))
      .map((lineage, versions) => lineage -> resolveLineage(lineage, versions))

  private def lineageOf(id: Identifier, isInterface: Boolean): Lineage =
    Lineage(
      id.packageName,
      id.moduleName,
      id.entityName,
      if isInterface then EntityKind.Interface else EntityKind.Template
    )

  private def fieldsOf(payload: Descriptor): Seq[(String, Descriptor)] =
    payload match
      case Descriptor.Record.Ctor(_, _, fields) => fields.map((n, d) => (n.fieldName, d))
      case r: Descriptor.Record                 => r.fields.map((n, d) => (n.fieldName, d))
      case _                                    => Seq.empty

  private def primitive(d: Descriptor): Option[(PgType, Option[Seq[String]])] =
    d match
      case Descriptor.Int64                      => Some((PgType.Bigint, None))
      case Descriptor.Bool                       => Some((PgType.Bool, None))
      case Descriptor.Text | Descriptor.Party    => Some((PgType.Text, None))
      case _: Descriptor.ContractId              => Some((PgType.Text, None))
      case Descriptor.Date                       => Some((PgType.Date, None))
      case Descriptor.Timestamp                  => Some((PgType.Timestamptz, None))
      case n: Descriptor.Numeric                 => Some((PgType.Numeric(n.scale), None))
      case Descriptor.Enumeration.Ctor(_, cases) => Some((PgType.Text, Some(cases.map(_.toString))))
      case e: Descriptor.Enumeration             => Some((PgType.Text, Some(e.cases.map(_.toString))))
      case _                                     => None

  private def classify(d: Descriptor): Option[(PgType, Boolean, Option[Seq[String]])] =
    d match
      case o: Descriptor.Optional => primitive(o.value).map((pg, ec) => (pg, true, ec))
      case other                  => primitive(other).map((pg, ec) => (pg, false, ec))

  private def unionEnumCases(sets: Seq[Seq[String]]): Option[Option[Seq[String]]] =
    sets.maxByOption(_.size) match
      case None => Some(None)
      case Some(longest) =>
        if sets.forall(s => longest.take(s.size).sameElements(s)) then Some(Some(longest)) else None

  private def resolveLineage(lineage: Lineage, versions: Seq[Template[Descriptor]]): ResolvedShape =
    val perVersion = versions.sortBy(_.templateId).map(t => fieldsOf(t.payload))
    val union      = perVersion.maxByOption(_.size).getOrElse(Seq.empty)
    val resolved = union.zipWithIndex.map { (field, i) =>
      val (name, _)  = field
      val present    = perVersion.filter(_.size > i).map(_(i))
      val namesOk    = present.map(_._1).distinct.size <= 1
      val classified = present.map((_, d) => classify(d))
      val typeShapes = classified.map(_.map((pg, nullable, _) => (pg, nullable))).distinct
      val enumUnion  = unionEnumCases(classified.flatMap(_.flatMap(_._3)))
      val diverges =
        (
          Left(name),
          Some(s"field '$name' at position $i diverges across versions of ${lineage.qualified}; kept in JSON")
        )
      (namesOk, typeShapes, enumUnion) match
        case (true, Seq(Some((pg, nullable))), Some(enumCases)) =>
          (Right(PromotedField(name, pg, nullable, i, enumCases)), None)
        case (true, Seq(None), _) =>
          (Left(name), None)
        case _ =>
          diverges
    }
    ResolvedShape(
      lineage,
      union.size,
      resolved.collect { case (Right(f), _) => f },
      resolved.collect { case (Left(n), _) => n },
      resolved.flatMap(_._2)
    )
end Shape
