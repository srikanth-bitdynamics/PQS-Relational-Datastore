// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.document

import com.digitalasset.canonical.*
import com.digitalasset.canonical.specific.Offset
import com.digitalasset.pqs.o11y.traces.DetachedSpan
import com.digitalasset.pqs.postgres.backend.IdPlaceholder
import com.digitalasset.pqs.postgres.document.specific
import zio.jdbc.JdbcDecoder
import zio.metrics.MetricLabel

import scala.language.implicitConversions

object model {
  export com.digitalasset.pqs.postgres.backend.encoding.*
  export com.digitalasset.pqs.postgres.backend.copy.*

  enum Table(val name: String) extends CopyTable:
    case Transactions extends Table("__transactions")
    case Events       extends Table("__events")
    case Contracts    extends Table("__contracts")
    case Exercises    extends Table("__exercises")
    // As opposed to the tables above, __archives is a view with an `instead of insert` trigger
    // `__insert_archive_trg` that updates the underlying __contracts row instead of inserting.
    case Archives extends Table("__archives")

  // Row-count trace attributes are recorded for these tables (empty ones report zero).
  val statTables: Seq[CopyTable] = Table.values.toIndexedSeq

  final class Transaction(tx: specific.Transaction, val span: Option[DetachedSpan] = None) extends TransactionCopy:
    val _table                   = Table.Transactions
    val _sql                     = s"/*0*/ copy ${_table.name} (${tx.columns.mkString(", ")}) from stdin"
    val _row                     = tx.rowValues
    val labels: Set[MetricLabel] = l("type" -> "transaction")
    val ix: Long                 = tx.ix
    val offset: Offset           = tx.offset

  opaque type EntityTypePk <: Long = Long
  object EntityTypePk:
    inline def apply(value: Long): EntityTypePk = value
    given JdbcDecoder[EntityTypePk]             = JdbcDecoder.longDecoder.map(apply)

  type PackagePk = Long

  final class Event(ev: specific.Event) extends Copy:
    val _table = Table.Events
    val _sql   = s"/*1*/ copy ${_table.name} (${ev.columns.mkString(", ")}) from stdin"
    val _row   = ev.rowValues
    val labels = Set.empty

  final class Contract(ev: specific.Contract) extends Copy:
    val _table = Table.Contracts
    val _sql   = s"/*2*/ copy ${_table.name} (${ev.columns.mkString(", ")}) from stdin"
    val _row   = ev.rowValues
    val labels: Set[MetricLabel] =
      l("type" -> "create", "template" -> ev.qualifiedName)

  final class Exercise(ev: specific.Exercise) extends Copy:
    val _table = Table.Exercises
    val _sql   = s"/*3*/ copy ${_table.name} (${ev.columns.mkString(", ")}) from stdin"
    val _row   = ev.rowValues
    val labels: Set[MetricLabel] =
      l("type" -> "exercise", "template" -> ev.qualifiedName, "choice" -> ev.choiceName)

  final class Archive(
      qualifiedName: String,
      entityType: EntityTypePk,
      eventPk: IdPlaceholder,
      txIx: Long,
      contractId: ContractId,
      packagePk: PackagePk
  ) extends Copy {
    val _table = Table.Archives
    val _sql =
      s"/*4*/ copy ${_table.name} (archive_event_pk, archived_at_ix, contract_id, tpe_pk, package_pk) from stdin"
    val _row = values(eventPk)(txIx)(contractId)(entityType)(packagePk)
    val labels: Set[MetricLabel] =
      l("type" -> "archive", "template" -> qualifiedName)
  }

  // document-specific value converters (the domain-neutral ones live in backend.encoding)
  given domainIdConverter: ValueConverter[DomainId] = value => value
  given tuple2Converter[A]: ValueConverter[(A, A)]  = value => s"(\"${value._1}\",\"${value._2}\")"

  enum EventType:
    case Create, Archive, Exercise, Assign, Unassign
  private[document] given eventTypeConverter: ValueConverter[EventType] =
    case EventType.Create   => "create"
    case EventType.Archive  => "archive"
    case EventType.Exercise => "exercise"
    case EventType.Assign   => "assign"
    case EventType.Unassign => "unassign"
}
