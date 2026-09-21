package com.digitalasset.pqs.postgres.relational.projection

object IndexPlanner:
  final case class OrderKey(column: String, ascending: Boolean)

  final case class IndexSpec(equality: Seq[String], ordered: Seq[OrderKey]):
    def columns: Seq[String] = equality ++ ordered.map(_.column)

  final case class Plan(indexes: Seq[IndexSpec], diagnostics: Seq[String])

  def parseOrder(token: String): OrderKey =
    val trimmed = token.trim
    val parts   = trimmed.split("\\s+").toVector
    parts.lastOption match
      case Some(direction) if isDirection(direction) =>
        OrderKey(parts.dropRight(1).mkString(" ").trim, !strEq(direction.toLowerCase, "desc"))
      case _ =>
        OrderKey(trimmed, true)

  def plan(queries: Seq[ProjectionQuery], available: Set[String]): Plan =
    val perQuery = queries.map { query =>
      val equality = query.filter.distinct.sorted
      val ordered  = query.order.map(parseOrder)
      val cols     = equality ++ ordered.map(_.column)
      val missing  = cols.filterNot(available.contains).distinct
      if missing.nonEmpty then
        (
          Option.empty[IndexSpec],
          Seq(s"query [${cols.mkString(", ")}] references unknown column(s): ${missing.mkString(", ")}; skipped")
        )
      else if equality.isEmpty && ordered.isEmpty then (Option.empty[IndexSpec], Seq.empty[String])
      else (Some(IndexSpec(equality, ordered)), Seq.empty[String])
    }
    val candidates  = perQuery.flatMap(_._1)
    val diagnostics = perQuery.flatMap(_._2)
    val deduped     = dedup(candidates)
    val reduced     = deduped.filterNot(candidate => dominated(candidate, deduped))
    val indexes     = reduced.sortBy(_.columns.mkString(","))
    Plan(indexes, diagnostics.sorted)

  private def dedup(specs: Seq[IndexSpec]): Seq[IndexSpec] =
    specs.foldLeft(Seq.empty[IndexSpec]) { (acc, candidate) =>
      if acc.exists(existing => indexSpecEq(existing, candidate)) then acc
      else acc :+ candidate
    }

  private def dominated(candidate: IndexSpec, all: Seq[IndexSpec]): Boolean =
    all.exists { other =>
      !indexSpecEq(candidate, other) &&
      candidate.equality.corresponds(other.equality)(strEq) &&
      orderedPrefixOf(candidate.ordered, other.ordered)
    }

  private def indexSpecEq(a: IndexSpec, b: IndexSpec): Boolean =
    a.equality.corresponds(b.equality)(strEq) && a.ordered.corresponds(b.ordered)(orderKeyEq)

  private def orderedPrefixOf(xs: Seq[OrderKey], ys: Seq[OrderKey]): Boolean =
    xs.length <= ys.length && xs.corresponds(ys.take(xs.length))(orderKeyEq)

  private def orderKeyEq(a: OrderKey, b: OrderKey): Boolean =
    strEq(a.column, b.column) && boolEq(a.ascending, b.ascending)

  private def isDirection(word: String): Boolean =
    val lower = word.toLowerCase
    strEq(lower, "asc") || strEq(lower, "desc")

  private def boolEq(a: Boolean, b: Boolean): Boolean =
    (a, b) match
      case (true, true)   => true
      case (false, false) => true
      case _              => false

  private def strEq(a: String, b: String): Boolean =
    a.compareTo(b) match
      case 0 => true
      case _ => false
end IndexPlanner
