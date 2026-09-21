package com.digitalasset.pqs.postgres.relational

import com.digitalasset.canonical.specific.Offset
import com.digitalasset.pqs.o11y.traces.DetachedSpan
import com.digitalasset.pqs.postgres.relational.specific
import com.digitalasset.transcode.schema.ChoiceName
import zio.metrics.MetricLabel

import scala.language.implicitConversions

object model {
  export com.digitalasset.pqs.postgres.backend.encoding.*
  export com.digitalasset.pqs.postgres.backend.copy.*

  enum Table(val name: String) extends CopyTable:
    case Transactions       extends Table("__rel_transactions")
    case Events             extends Table("__query_events")
    case EventVisibility    extends Table("__query_event_visibility")
    case Contracts          extends Table("__rel_contracts")
    case ContractVisibility extends Table("__rel_contract_visibility")
    case Exercises          extends Table("__rel_exercises")
    case TmpLifecycle       extends Table("__rel_tmp_lifecycle")
    // per-template payload tables are named dynamically; these cases carry only the metrics label
    case ContractPayload extends Table("rel_contract_payload")
    case InterfaceView   extends Table("relv_interface_view")

  // Row-count trace attributes are recorded for these tables (empty ones report zero).
  val statTables: Seq[CopyTable] =
    Seq(Table.Transactions, Table.Events, Table.Contracts, Table.Exercises)

  final class Transaction(tx: specific.Transaction, val span: Option[DetachedSpan] = None) extends TransactionCopy:
    val _table                   = Table.Transactions
    val _sql                     = s"/*0*/ copy ${_table.name} (${tx.columns.mkString(", ")}) from stdin"
    val _row                     = tx.rowValues
    val labels: Set[MetricLabel] = l("type" -> "transaction")
    val ix: Long                 = tx.ix
    val offset: Offset           = tx.offset

  final class Event(ev: specific.Event) extends Copy:
    val _table = Table.Events
    val _sql   = s"/*1*/ copy ${_table.name} (${ev.columns.mkString(", ")}) from stdin"
    val _row   = ev.rowValues
    val labels = Set.empty

  final class EventVisibility(ev: specific.EventVisibility) extends Copy:
    val _table = Table.EventVisibility
    val _sql   = s"/*2*/ copy ${_table.name} (${ev.columns.mkString(", ")}) from stdin"
    val _row   = ev.rowValues
    val labels = Set.empty

  final class Contract(ev: specific.Contract) extends Copy:
    val _table                   = Table.Contracts
    val _sql                     = s"/*3*/ copy ${_table.name} (${ev.columns.mkString(", ")}) from stdin"
    val _row                     = ev.rowValues
    val labels: Set[MetricLabel] = l("type" -> "create")

  final class ContractVisibility(ev: specific.ContractVisibility) extends Copy:
    val _table = Table.ContractVisibility
    val _sql   = s"/*4*/ copy ${_table.name} (${ev.columns.mkString(", ")}) from stdin"
    val _row   = ev.rowValues
    val labels = Set.empty

  final class Exercise(ev: specific.Exercise) extends Copy:
    val _table                   = Table.Exercises
    val _sql                     = s"/*5*/ copy ${_table.name} (${ev.columns.mkString(", ")}) from stdin"
    val _row                     = ev.rowValues
    val labels: Set[MetricLabel] = l("type" -> "exercise")

  final class TmpLifecycle(ev: specific.TmpLifecycle) extends Copy:
    val _table                   = Table.TmpLifecycle
    val _sql                     = s"/*6*/ copy ${_table.name} (${ev.columns.mkString(", ")}) from stdin"
    val _row                     = ev.rowValues
    val labels: Set[MetricLabel] = l("type" -> "archive")

  final class ContractPayload(ev: specific.ContractPayload, tableName: String) extends Copy:
    val _table                   = Table.ContractPayload
    val _sql                     = s"/*7*/ copy $tableName (${ev.columns.mkString(", ")}) from stdin"
    val _row                     = ev.rowValues
    val labels: Set[MetricLabel] = l("type" -> "payload")

  final class InterfaceView(ev: specific.InterfaceView, tableName: String) extends Copy:
    val _table = Table.InterfaceView
    val _sql   = s"/*8*/ copy $tableName (${ev.columns.mkString(", ")}) from stdin"
    val _row   = ev.rowValues
    val labels = Set.empty

  // relational-specific value converters (the domain-neutral ones live in backend.encoding)
  given choiceNameConverter: ValueConverter[ChoiceName] = value => escape(value.toString)

  enum EventKind:
    case Create; case Exercise; case Archive
  given eventKindConverter: ValueConverter[EventKind] =
    case EventKind.Create   => "create"
    case EventKind.Exercise => "exercise"
    case EventKind.Archive  => "archive"

  enum SourceKind:
    case Stream; case AcsSeed; case LedgerReplay; case DocumentBackfill
  given sourceKindConverter: ValueConverter[SourceKind] =
    case SourceKind.Stream           => "stream"
    case SourceKind.AcsSeed          => "acs_seed"
    case SourceKind.LedgerReplay     => "ledger_replay"
    case SourceKind.DocumentBackfill => "document_backfill"

  enum ArchiveSource:
    case Native; case ConsumingExercise
  given archiveSourceConverter: ValueConverter[ArchiveSource] =
    case ArchiveSource.Native            => "native"
    case ArchiveSource.ConsumingExercise => "consuming_exercise"

  enum VisibilityRole:
    case Signatory; case Observer; case Witness
  given visibilityRoleConverter: ValueConverter[VisibilityRole] =
    case VisibilityRole.Signatory => "signatory"
    case VisibilityRole.Observer  => "observer"
    case VisibilityRole.Witness   => "witness"
}
