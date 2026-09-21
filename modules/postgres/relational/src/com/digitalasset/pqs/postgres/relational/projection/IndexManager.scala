// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.relational.projection

import com.digitalasset.pqs.utils.safeequals.===
import com.digitalasset.pqs.postgres.backend.{transact as transaction}
import ujson.Value
import zio.ZIO
import zio.jdbc.*

object IndexManager:
  import IndexPlanner.OrderKey

  val systemColumns: Set[String]    = Set("created_tx_ix", "archived_tx_ix")
  val contractsColumns: Set[String] = Set("created_at_offset", "contract_pk")
  private val includeColumn         = "contract_pk"

  final case class CoveredShape(
      filter: Seq[String],
      order: Seq[OrderKey],
      physical: Seq[String],
      filterComplete: Boolean,
      orderExternal: Boolean,
      fullCoverage: Boolean,
      residual: Seq[OrderKey]
  )

  final case class PlannedIndex(
      table: String,
      name: String,
      keys: Seq[OrderKey],
      include: Seq[String],
      covered: Seq[CoveredShape]
  ):
    def columns: Seq[String] = keys.map(_.column)

    def definition: String = definitionOn(quoteIdent(table))

    def definitionIn(schema: String): String = definitionOn(s"${quoteIdent(schema)}.${quoteIdent(table)}")

    private def definitionOn(target: String): String =
      val cols = keys.map(k => quoteIdent(k.column) + (if k.ascending then "" else " desc")).mkString(", ")
      val incl = include.map(quoteIdent).mkString(", ")
      s"create index concurrently if not exists ${quoteIdent(name)} on $target ($cols) include ($incl)"

  final case class Plan(indexes: Seq[PlannedIndex], diagnostics: Seq[String], notes: Seq[String])

  private final case class Classified(
      filter: Seq[String],
      order: Seq[OrderKey],
      payloadFilter: Seq[String],
      externalFilter: Seq[String],
      payloadOrder: Seq[OrderKey],
      residualOrder: Seq[OrderKey],
      unknown: Seq[String]
  )

  def plan(
      definitions: Map[String, ProjectionDefinition],
      shapes: Map[String, Map[String, Seq[String]]],
      baseTables: Map[String, String]
  ): Plan =
    val perName = definitions.toSeq.sortBy(_._1).map { (name, definition) =>
      planProjection(name, definition, shapes.getOrElse(name, Map.empty), baseTables)
    }
    Plan(
      perName
        .flatMap(_._1)
        .groupMapReduce(i => (i.table, i.name))(identity)((a, b) => a.copy(covered = (a.covered ++ b.covered).distinct))
        .values
        .toSeq
        .sortBy(i => (i.table, i.name)),
      perName.flatMap(_._2).distinct.sorted,
      perName.flatMap(_._3).distinct.sorted
    )

  private def planProjection(
      name: String,
      definition: ProjectionDefinition,
      shapesForName: Map[String, Seq[String]],
      baseTables: Map[String, String]
  ): (Seq[PlannedIndex], Seq[String], Seq[String]) =
    val perTemplate = definition.templates.distinct.sorted.map { qualified =>
      baseTables.get(qualified) match
        case None => (Seq.empty[PlannedIndex], Seq.empty[String], Seq.empty[String])
        case Some(table) =>
          val promoted = shapesForName.getOrElse(qualified, Seq.empty).toSet
          planTable(name, qualified, table, promoted, definition.queries)
    }
    (perTemplate.flatMap(_._1), perTemplate.flatMap(_._2), perTemplate.flatMap(_._3))

  private def planTable(
      name: String,
      qualified: String,
      table: String,
      promoted: Set[String],
      queries: Seq[ProjectionQuery]
  ): (Seq[PlannedIndex], Seq[String], Seq[String]) =
    val local       = promoted ++ systemColumns
    val classifieds = queries.map(classify(_, local))
    val diagnostics = classifieds.filter(_.unknown.nonEmpty).map { c =>
      s"projection '$name' on $qualified: query ${shapeText(c)} references non-promoted column(s): " +
        c.unknown.distinct.sorted.mkString(", ") + "; skipped"
    }
    val usable                = classifieds.filter(_.unknown.isEmpty)
    val (indexable, external) = usable.partition(c => c.payloadFilter.nonEmpty || c.payloadOrder.nonEmpty)
    val notes = external.map(c =>
      s"projection '$name' on $qualified: query ${shapeText(c)} has no payload-indexable columns; " +
        "served by __rel_contracts lifecycle indexes"
    )
    val payloadReqs = indexable.map(c => ProjectionQuery(c.payloadFilter, c.payloadOrder.map(tokenOf)))
    val specs       = IndexPlanner.plan(payloadReqs, local).indexes
    val attributed  = indexable.map(c => (c, specs.filter(covers(_, c)).maxByOption(_.columns.length)))
    val indexes = specs.flatMap { spec =>
      attributed.collect { case (c, Some(s)) if s === spec => c } match
        case Seq() => Seq.empty[PlannedIndex]
        case forSpec =>
          val keys = spec.equality.map(OrderKey(_, true)) ++ spec.ordered
          Seq(PlannedIndex(table, indexName(table, keys), keys, Seq(includeColumn), forSpec.map(covered(_, spec))))
    }
    (indexes, diagnostics, notes)

  private def classify(query: ProjectionQuery, local: Set[String]): Classified =
    val parsed         = query.order.map(IndexPlanner.parseOrder)
    val payloadFilter  = query.filter.filter(local.contains)
    val externalFilter = query.filter.filter(contractsColumns.contains)
    val unknownFilter  = query.filter.filterNot(c => local.contains(c) || contractsColumns.contains(c))
    val payloadOrder   = parsed.takeWhile(k => local.contains(k.column))
    val rest           = parsed.drop(payloadOrder.length)
    val residualOrder  = rest.filter(k => contractsColumns.contains(k.column))
    val unknownOrder   = rest.filterNot(k => contractsColumns.contains(k.column)).map(_.column)
    Classified(
      query.filter,
      parsed,
      payloadFilter,
      externalFilter,
      payloadOrder,
      residualOrder,
      unknownFilter ++ unknownOrder
    )

  private def covered(c: Classified, spec: IndexPlanner.IndexSpec): CoveredShape =
    CoveredShape(
      c.filter,
      c.order,
      spec.columns,
      c.externalFilter.isEmpty,
      c.residualOrder.nonEmpty,
      c.externalFilter.isEmpty && c.residualOrder.isEmpty,
      c.residualOrder
    )

  private def covers(spec: IndexPlanner.IndexSpec, c: Classified): Boolean =
    val ordered = c.payloadOrder.filterNot(k => c.payloadFilter.contains(k.column)).distinctBy(_.column)
    (spec.equality.toSet === c.payloadFilter.toSet) && spec.ordered.startsWith(ordered)

  private def tokenOf(k: OrderKey): String = if k.ascending then k.column else s"${k.column} desc"

  private def shapeText(c: Classified): String =
    s"filter=[${c.filter.mkString(", ")}] order=[${c.order.map(tokenOf).mkString(", ")}]"

  private def quoteIdent(name: String): String = "\"" + name.replace("\"", "\"\"") + "\""

  private def indexName(table: String, keys: Seq[OrderKey]): String =
    val tokens = keys.map(k => if k.ascending then k.column else s"${k.column}_desc")
    val raw    = ujson.Arr(ujson.Str(table), ujson.Arr.from(keys.map(k => ujson.Arr(k.column, k.ascending))))
    val slug   = s"${table}_${tokens.mkString("_")}".toLowerCase.replaceAll("[^a-z0-9]", "_")
    val prefix = "rix_"
    val budget = 63 - prefix.length - 14
    s"$prefix${slug.take(budget)}_h${ProjectionDefinition.canonicalHash(raw).take(12)}"

  private def shapeJson(s: CoveredShape): Value =
    val requested = s.filter.map(f => ujson.Obj("column" -> f, "kind" -> "eq")) ++
      s.order.map(k =>
        ujson.Obj("column" -> k.column, "kind" -> "order", "direction" -> (if k.ascending then "asc" else "desc"))
      )
    val orderCoverage = if s.order.isEmpty then "absent" else if s.orderExternal then "external" else "complete"
    ujson.Obj(
      "queryShape"     -> ujson.Arr(requested*),
      "physicalIndex"  -> ujson.Arr(s.physical.map(ujson.Str(_))*),
      "filterCoverage" -> (if s.filterComplete then "complete" else "partial"),
      "orderCoverage"  -> orderCoverage,
      "fullCoverage"   -> s.fullCoverage,
      "residual" -> ujson.Arr(
        s.residual.map(k =>
          ujson.Obj(
            "column"    -> k.column,
            "direction" -> (if k.ascending then "asc" else "desc"),
            "relation"  -> "__rel_contracts"
          )
        )*
      )
    )

  private enum Validation:
    case Valid(name: String)
    case Invalid(name: String)
    case Missing(name: String)
    case Mismatch(name: String)

  private final case class Active(
      version: Long,
      definitions: Map[String, ProjectionDefinition],
      shapes: Map[String, Map[String, Seq[String]]]
  )

  def build: ZIO[ZConnectionPool, Throwable, String] =
    withLifecycleLock(planActive.flatMap {
      case None => ZIO.succeed("No active projection; nothing to build")
      case Some((version, p)) =>
        for
          schema <- currentSchema
          _      <- ZIO.foreachDiscard(p.indexes)(insertBuilding(version, _))
          failures <- ZIO.foreach(p.indexes)(idx =>
            (dropIfInvalid(schema, idx) *> runConcurrently(idx.definitionIn(schema))).either.map(idx.name -> _)
          )
          outcomes <- ZIO.foreach(p.indexes)(validateOne)
          invalid = outcomes.toSeq.collect {
            case Validation.Invalid(n)  => s"$n is not ready/valid; inspect the failed build before retrying"
            case Validation.Missing(n)  => s"$n is missing from the configured schema"
            case Validation.Mismatch(n) => s"$n does not match the registered table and planned definition"
          }
          errors = failures.collect { case (name, Left(error)) => s"$name: ${error.getMessage}" } ++ invalid
          _ <- ZIO
            .fail(new RuntimeException(errors.mkString("Index build failed: ", "; ", "")))
            .when(errors.nonEmpty)
        yield renderBuild(p)
    })

  def adopt: ZIO[ZConnectionPool, Throwable, String] =
    withLifecycleLock {
      planActive.flatMap {
        case None => ZIO.succeed("No active projection; nothing to adopt")
        case Some((version, p)) =>
          ZIO.foreach(p.indexes)(validateOne).flatMap { outcomes =>
            val complete = outcomes.forall {
              case Validation.Valid(_) => true
              case _                   => false
            }
            if !complete then
              ZIO.succeed(
                "Adopted 0 index(es); retirement deferred until all planned indexes are valid (existing coverage kept)"
              )
            else
              for
                adopted <- (sql"""update __rel_managed_index
                                    set projection_version = $version, status = 'active'::rel_index_status, adopted = true
                                    where status in ('valid'::rel_index_status, 'active'::rel_index_status)
                                      and index_name = any(""" ++ textArray(p.indexes.map(_.name)) ++ sql")").update
                superseded <- supersede(p.indexes.map(_.name))
              yield s"Adopted $adopted index(es); marked $superseded obsolete index(es) for retirement"
          }
      }
    }

  def retire: ZIO[ZConnectionPool, Throwable, String] =
    withLifecycleLock {
      for
        schema <- currentSchema
        active <- planActive
        planned = active.toList.flatMap(_._2.indexes.map(_.name)).toSet
        candidates <-
          sql"select index_name, table_name, physical_oid::bigint from __rel_managed_index where status = 'retiring'::rel_index_status"
            .query[(String, String, Option[Long])]
            .selectAll
            .map(_.toSeq)
        results <- ZIO.foreach(candidates.filterNot(c => planned.contains(c._1))) { (name, table, oid) =>
          (verifyOwnership(schema, name, table, oid) *> runConcurrently(dropDdl(schema, name))).either.map(name -> _)
        }
        dropped  = results.collect { case (n, Right(_)) => n }
        failures = results.collect { case (name, Left(error)) => s"$name: ${error.getMessage}" }
        retired <-
          (sql"""update __rel_managed_index set status = 'retired'::rel_index_status
               where status = 'retiring'::rel_index_status and index_name = any(""" ++ textArray(
            dropped
          ) ++ sql")").update
      yield
        if results.size > dropped.size then
          s"Retired $retired index(es); ${failures.size} drop(s) failed and remain retiring for retry: ${failures.mkString("; ")}"
        else if candidates.size > results.size then
          s"Retired $retired index(es); kept ${candidates.size - results.size} index(es) required by the active projection"
        else s"Retired $retired index(es)"
    }

  def listReport: ZIO[ZConnectionPool, Throwable, String] =
    transaction(listRows).map { rows =>
      rows.toSeq match
        case Seq() => "No managed indexes"
        case ordered =>
          ordered
            .map((v, table, name, status, adopted) => s"v$v\t$table\t$name\t$status\tadopted=$adopted")
            .mkString(System.lineSeparator)
    }

  def planReport: ZIO[ZConnectionPool, Throwable, String] =
    transaction(planActive).map {
      case None               => "No active projection"
      case Some((_, planned)) => renderPlan(planned)
    }

  private def planActive: ZIO[ZConnection, Throwable, Option[(Long, Plan)]] =
    loadActive.flatMap {
      case None    => ZIO.none
      case Some(a) => baseTables.map(bt => Some((a.version, plan(a.definitions, a.shapes, bt))))
    }

  private def loadActive: ZIO[ZConnection, Throwable, Option[Active]] =
    sql"""select projection_version, definition::text, resolved_shape::text
          from __query_projection where status = 'active'"""
      .query[(Long, String, String)]
      .selectOne
      .map(
        _.map((version, definition, shape) =>
          Active(version, ProjectionDefinition.fromJson(ujson.read(definition)), parseShapes(ujson.read(shape)))
        )
      )

  private def parseShapes(json: Value): Map[String, Map[String, Seq[String]]] =
    json.obj.map { (name, byQualified) =>
      name -> byQualified.obj.map((qualified, cols) => qualified -> cols.arr.map(_("name").str).toSeq).toMap
    }.toMap

  private def baseTables: ZIO[ZConnection, Throwable, Map[String, String]] =
    sql"""select package_name || ':' || module_name || ':' || entity_name, base_table
          from __rel_entity where kind = 'template' and base_table is not null"""
      .query[(String, String)]
      .selectAll
      .map(_.toMap)

  private def insertBuilding(version: Long, idx: PlannedIndex): ZIO[ZConnection, Throwable, Unit] =
    val shapes = textArray(idx.covered.map(s => ujson.write(shapeJson(s))))
    val ensureManaged = sql"""select exists (
          select 1 from pg_class c join pg_namespace n on n.oid = c.relnamespace
          where n.nspname = current_schema() and c.relname = ${idx.name}
        ) and not exists (
          select 1 from __rel_managed_index
          where index_name = ${idx.name} and table_name = ${idx.table} and definition = ${idx.definition}
            and status <> 'retired'::rel_index_status
        )""".query[Boolean].selectOne.flatMap { collision =>
      ZIO
        .fail(new RuntimeException(s"Refusing unmanaged index name collision: ${idx.name}"))
        .when(collision.contains(true))
    }
    // A re-planned retiring index must re-enter validation before it can be adopted again.
    val refresh = (sql"""update __rel_managed_index m
          set projection_version = $version, covered_query_shapes = """ ++ shapes ++ sql""",
              physical_oid = case when p.present then m.physical_oid else null end,
              status = case
                when p.present and m.status = 'retiring'::rel_index_status then 'building'::rel_index_status
                when p.present then m.status
                else 'building'::rel_index_status end
          from (select exists (
            select 1 from pg_class c join pg_namespace n on n.oid = c.relnamespace
            where n.nspname = current_schema() and c.relname = ${idx.name}) as present) p
          where m.index_name = ${idx.name} and m.table_name = ${idx.table} and m.definition = ${idx.definition}
            and m.status <> 'retired'::rel_index_status""").update
    ensureManaged *> refresh *> (sql"""insert into __rel_managed_index
             (projection_version, table_name, index_name, definition, columns, opclasses,
              status, adopted, covered_query_shapes, created_at)
           select $version, ${idx.table}, ${idx.name}, ${idx.definition}, """ ++ textArray(idx.columns) ++
      sql", null, 'building'::rel_index_status, false, " ++ shapes ++ sql""", now()
           where not exists (
             select 1 from __rel_managed_index m
             where m.index_name = ${idx.name} and m.status <> 'retired'::rel_index_status)""").update.unit

  private def validateOne(idx: PlannedIndex): ZIO[ZConnection, Throwable, Validation] =
    val directions = idx.keys.map(k => if k.ascending then "0" else "3")
    (sql"""select i.indisvalid and i.indisready,
            t.relnamespace = c.relnamespace and t.relname = ${idx.table}
            and am.amname = 'btree' and not i.indisunique and not i.indisprimary and not i.indisexclusion
            and i.indpred is null and i.indexprs is null
            and i.indnkeyatts = ${idx.keys.size} and i.indnatts = ${idx.keys.size + idx.include.size}
            and array(select a.attname::text from unnest(i.indkey) with ordinality k(attnum, pos)
                      join pg_attribute a on a.attrelid = t.oid and a.attnum = k.attnum
                      order by k.pos) = """ ++ textArray(idx.columns ++ idx.include) ++
      sql""" and array(select opt::text from unnest(i.indoption) opt) = """ ++ textArray(directions) ++
      sql""" and not exists (
              select 1 from unnest(i.indkey) with ordinality k(attnum, pos)
              join pg_attribute a on a.attrelid = t.oid and a.attnum = k.attnum
              join pg_opclass opc on opc.oid = i.indclass[k.pos::int - 1]
              where k.pos <= i.indnkeyatts
                and (not opc.opcdefault or i.indcollation[k.pos::int - 1] <> a.attcollation)
            ) and exists (
              select 1 from __rel_managed_index m where m.index_name = ${idx.name}
                and m.table_name = ${idx.table} and m.definition = ${idx.definition}
            and m.status in ('building'::rel_index_status, 'valid'::rel_index_status, 'active'::rel_index_status)
            and (m.physical_oid is null or m.physical_oid = c.oid)
            )
          from pg_class c join pg_namespace n on n.oid = c.relnamespace
          join pg_index i on i.indexrelid = c.oid join pg_class t on t.oid = i.indrelid
          join pg_am am on am.oid = c.relam
          where n.nspname = current_schema() and c.relname = ${idx.name}""")
      .query[(Boolean, Boolean)]
      .selectOne
      .flatMap {
        case Some((true, true)) =>
          sql"""update __rel_managed_index
                set status = case when status = 'active'::rel_index_status then status else 'valid'::rel_index_status end,
                    validated_at = now(), physical_oid = (
                      select c.oid from pg_class c join pg_namespace n on n.oid = c.relnamespace
                      where n.nspname = current_schema() and c.relname = ${idx.name})
                where index_name = ${idx.name}
                  and status in ('building'::rel_index_status, 'valid'::rel_index_status, 'active'::rel_index_status)""".update
            .as(Validation.Valid(idx.name))
        case Some((_, false)) => ZIO.succeed(Validation.Mismatch(idx.name))
        case Some((false, _)) => ZIO.succeed(Validation.Invalid(idx.name))
        case None             => ZIO.succeed(Validation.Missing(idx.name))
      }

  private def supersede(names: Seq[String]): ZIO[ZConnection, Throwable, Long] =
    (sql"""update __rel_managed_index set status = 'retiring'::rel_index_status, adopted = false
           where status in ('active'::rel_index_status, 'valid'::rel_index_status, 'building'::rel_index_status)
             and index_name <> all(""" ++ textArray(names) ++ sql")").update

  private def listRows: ZIO[ZConnection, Throwable, Seq[(Long, String, String, String, Boolean)]] =
    sql"""select coalesce(projection_version, 0), table_name, index_name, status::text, adopted
          from __rel_managed_index order by index_id"""
      .query[(Long, String, String, String, Boolean)]
      .selectAll
      .map(_.toSeq)

  private def withLifecycleLock[A](body: ZIO[ZConnection, Throwable, A]): ZIO[ZConnectionPool, Throwable, A] =
    ZIO.serviceWithZIO[ZConnectionPool] { pool =>
      ZIO.scoped {
        for
          env <- pool.transaction.build
          connection = env.get[ZConnection]
          _ <- ZIO.acquireRelease(
            (sql"set local lock_timeout = '30s'".execute *>
              sql"select 1 from pg_advisory_lock(${ProjectionRegistry.projectionLockKey})".query[Int].selectOne)
              .provideEnvironment(env)
          ) { _ =>
            (connection.rollback *>
              sql"select pg_advisory_unlock(${ProjectionRegistry.projectionLockKey})"
                .query[Boolean]
                .selectOne
                .provideEnvironment(env) *>
              connection.access(_.commit())).catchAll(_ => pool.invalidate(connection))
          }
          _      <- connection.access(_.commit())
          result <- body.provideEnvironment(env)
          _      <- connection.access(_.commit())
        yield result
      }
    }

  private def runConcurrently(ddl: String): ZIO[ZConnection, Throwable, Unit] =
    ZIO.serviceWithZIO[ZConnection](_.access { c =>
      val previous = c.getAutoCommit
      if !previous then c.commit()
      c.setAutoCommit(true)
      try
        val statement = c.createStatement()
        try
          statement.execute(ddl)
          ()
        finally statement.close()
      finally c.setAutoCommit(previous)
    })

  private def currentSchema: ZIO[ZConnection, Throwable, String] =
    sql"select current_schema()".query[String].selectOne.someOrFail(new RuntimeException("No current database schema"))

  private def verifyOwnership(
      schema: String,
      name: String,
      table: String,
      expectedOid: Option[Long]
  ): ZIO[ZConnection, Throwable, Unit] =
    sql"""select c.oid::bigint, coalesce(t.relname, '')::text, coalesce(t.relnamespace = c.relnamespace, false)
          from pg_class c join pg_namespace n on n.oid = c.relnamespace
          left join pg_index i on i.indexrelid = c.oid left join pg_class t on t.oid = i.indrelid
          where n.nspname = $schema and c.relname = $name"""
      .query[(Long, String, Boolean)]
      .selectOne
      .flatMap {
        case None => ZIO.unit
        case Some((oid, actualTable, sameSchema))
            if expectedOid.contains(oid) && (actualTable === table) && sameSchema =>
          ZIO.unit
        case _ =>
          ZIO.fail(
            new RuntimeException(s"Refusing to drop $schema.$name: physical index ownership is unverified or changed")
          )
      }

  private def dropDdl(schema: String, name: String): String =
    s"drop index concurrently if exists ${quoteIdent(schema)}.${quoteIdent(name)}"

  private def dropIfInvalid(schema: String, idx: PlannedIndex): ZIO[ZConnection, Throwable, Unit] =
    validateOne(idx).flatMap {
      case Validation.Invalid(_) =>
        runConcurrently(dropDdl(schema, idx.name)) *>
          sql"""update __rel_managed_index set physical_oid = null, status = 'building', adopted = false
                 where index_name = ${idx.name} and status <> 'retired'""".update.unit
      case Validation.Mismatch(_) =>
        ZIO.fail(new IllegalStateException(s"Refusing to replace mismatched managed index: ${idx.name}"))
      case _ => ZIO.unit
    }

  private def textArray(items: Seq[String]): SqlFragment =
    items match
      case Seq() => sql"array[]::text[]"
      case _     => sql"array[" ++ items.map(i => sql"$i").mkFragment(sql", ") ++ sql"]::text[]"

  private def renderBuild(planned: Plan): String =
    (Seq(s"Validated ${planned.indexes.size} planned index(es)") ++
      planned.diagnostics.map("  " + _) ++ planned.notes.map("  " + _))
      .mkString(System.lineSeparator)

  private def renderPlan(planned: Plan): String =
    val indexLines = planned.indexes.map { idx =>
      val shapeLines = idx.covered.map { s =>
        val order = if s.order.isEmpty then "absent" else if s.orderExternal then "external" else "complete"
        s"    shape filter=[${s.filter.mkString(", ")}] order=[${s.order.map(tokenOf).mkString(", ")}]" +
          s" -> filter=${if s.filterComplete then "complete" else "partial"} order=$order full=${s.fullCoverage}"
      }
      (s"  ${idx.name} on ${idx.table} (${idx.columns.mkString(", ")}) include (${idx.include.mkString(", ")})" +:
        shapeLines).mkString(System.lineSeparator)
    }
    val diag = planned.diagnostics.map("  diagnostic: " + _)
    val note = planned.notes.map("  note: " + _)
    (Seq(s"Planned ${planned.indexes.size} index(es):") ++ indexLines ++ diag ++ note).mkString(System.lineSeparator)
end IndexManager
