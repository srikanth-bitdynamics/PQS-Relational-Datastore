package com.digitalasset.pqs.postgres.relational.projection

import com.digitalasset.transcode.schema.Schema
import ujson.Value
import zio.ZIO
import zio.jdbc.*

object ProjectionApply:
  final case class PlannedColumn(lineage: Shape.Lineage, field: Shape.PromotedField)

  final case class Plan(
      definition: Value,
      resolvedShape: Value,
      hash: String,
      columns: Seq[PlannedColumn],
      diagnostics: Seq[String],
      errors: Seq[String]
  )

  enum Outcome:
    case Applied(version: Long, columns: Int, diagnostics: Seq[String])
    case AlreadyApplied(version: Long, diagnostics: Seq[String])

  private val layout = 1

  def plan(config: Map[String, ProjectionDefinition], schema: Schema): Plan =
    val canonicalConfig = config.view.mapValues(ProjectionDefinition.canonical).toMap
    val byQualified = Shape.resolveAll(schema).collect {
      case (lineage @ Shape.Lineage(_, _, _, Shape.EntityKind.Template), shape) => lineage.qualified -> shape
    }
    val perProjection = canonicalConfig.toSeq.sortBy(_._1).map { (name, definition) =>
      val promoteSet = definition.promote.toSet
      val resolved = definition.templates.map { template =>
        byQualified.get(template) match
          case None =>
            (Option.empty[(Shape.Lineage, Seq[Shape.PromotedField])], Seq.empty[String])
          case Some(shape) =>
            val columns   = shape.promoted.filter(f => promoteSet.contains(f.name))
            val available = shape.promoted.map(_.name).toSet
            val missing = promoteSet
              .diff(available)
              .toSeq
              .sorted
              .map(field => s"projection '$name': field '$field' of '$template' is not promotable; kept in JSON")
            (Some((shape.lineage, columns)), missing)
      }
      (name, resolved)
    }
    val columns = perProjection.flatMap((_, resolved) =>
      resolved.flatMap(_._1).flatMap((lineage, cols) => cols.map(PlannedColumn(lineage, _)))
    )
    val diagnostics = perProjection.flatMap((_, resolved) => resolved.flatMap(_._2))
    val errors = canonicalConfig.toSeq.sortBy(_._1).flatMap { (name, definition) =>
      definition.templates
        .filterNot(byQualified.contains)
        .map(template => s"projection '$name': template '$template' not found on the ledger")
    }
    val resolvedShape = shapeJson(perProjection)
    val definition    = ProjectionDefinition.toJson(config)
    val hashInput = ujson.Obj(
      "definition" -> ProjectionDefinition.toJson(canonicalConfig),
      "shape"      -> resolvedShape,
      "layout"     -> ujson.Num(layout)
    )
    val hash = ProjectionDefinition.canonicalHash(hashInput)
    Plan(definition, resolvedShape, hash, columns, diagnostics, errors)

  def apply(config: Map[String, ProjectionDefinition], schema: Schema): ZIO[ZConnection, Throwable, Outcome] =
    val p = plan(config, schema)
    if p.errors.nonEmpty then ZIO.fail(new RuntimeException(s"projection apply failed: ${p.errors.mkString("; ")}"))
    else
      sql"select 1 from pg_advisory_xact_lock(${ProjectionRegistry.projectionLockKey})".query[Int].selectOne *>
        ProjectionRegistry.getByHash(p.hash).flatMap {
          case Some(row) => ZIO.succeed(Outcome.AlreadyApplied(row.version, p.diagnostics))
          case None =>
            ProjectionRegistry.retireDrafts *>
              ZIO.foreachDiscard(p.columns)(promoteColumn) *>
              ProjectionRegistry
                .insertDraft(p.definition, p.hash, p.resolvedShape, layout)
                .map(version => Outcome.Applied(version, p.columns.size, p.diagnostics))
        }

  def render(outcome: Outcome): String =
    val (headline, diagnostics) = outcome match
      case Outcome.Applied(version, columns, diags) =>
        (s"Applied projection version $version ($columns typed columns)", diags)
      case Outcome.AlreadyApplied(version, diags) =>
        (s"Projection already applied as version $version; no changes", diags)
    (headline +: diagnostics).mkString(System.lineSeparator)

  private def promoteColumn(column: PlannedColumn): ZIO[ZConnection, Throwable, Unit] =
    val lineage = column.lineage
    val kind = lineage.kind match
      case Shape.EntityKind.Template  => "template"
      case Shape.EntityKind.Interface => "interface"
    sql"""call __rel_promote_column(${lineage.packageName}, ${lineage.moduleName}, ${lineage.entityName},
          ${kind}::rel_entity_kind, ${column.field.name}, ${column.field.pgType.sql})""".execute.unit

  private def colJson(f: Shape.PromotedField): Value =
    val base = ujson.Obj(
      "name"     -> f.name,
      "type"     -> f.pgType.sql,
      "nullable" -> f.nullable,
      "position" -> f.position
    )
    f.enumCases.foreach(cases => base("enum") = ujson.Arr(cases.map(ujson.Str(_))*))
    base

  private def shapeJson(
      perProjection: Seq[(String, Seq[(Option[(Shape.Lineage, Seq[Shape.PromotedField])], Seq[String])])]
  ): Value =
    ujson.Obj.from(perProjection.map { (name, resolved) =>
      name -> ujson.Obj.from(resolved.flatMap(_._1).map { (lineage, cols) =>
        lineage.qualified -> ujson.Arr(cols.map(colJson)*)
      })
    })
end ProjectionApply
