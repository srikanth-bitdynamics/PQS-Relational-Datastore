package com.digitalasset.pqs.postgres.relational

import com.digitalasset.canonical.*
import com.digitalasset.canonical.specific.Offset
import com.digitalasset.pqs.o11y.traces
import com.digitalasset.pqs.o11y.traces.{DetachedSpan, given}
import com.digitalasset.pqs.postgres.backend.IdPlaceholder
import com.digitalasset.pqs.postgres.relational.specific
import com.digitalasset.pqs.postgres.relational.specific.ValueConverter
import com.digitalasset.transcode.schema.ChoiceName
import io.opentelemetry.api.trace.SpanContext
import org.apache.commons.text.translate.LookupTranslator
import org.postgresql.PGConnection
import ujson.Value
import zio.ZIO.logTrace
import zio.jdbc.ZConnection
import zio.jdbc.shims.postgres.PGRestorableConnection
import zio.metrics.{Metric, MetricLabel}
import zio.{Chunk, ChunkBuilder, ZIO}

import java.io.StringReader
import java.lang.System.lineSeparator
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

object model {
  sealed trait Model { def labels: Set[MetricLabel] }
  sealed trait Copy extends Model { def _table: Table; def _sql: String; def _row: String }
  final case class Watermark(
      ix: Long,
      offset: Offset,
      seenAts: Seq[Long],
      txSpans: Seq[DetachedSpan] = Seq.empty,
      persistSpans: Seq[SpanContext] = Seq.empty
  ) extends Model {
    val labels = l("type" -> "watermark")
  }

  given watermarkOrdering: Ordering[Watermark] = Ordering.by(_.ix)
  private def l(kv: (String, Any)*)            = kv.map((k, v) => MetricLabel(k, v.toString)).toSet

  object Model {
    private val counter = Metric.counter("pipeline_events", "Processed ledger events")

    def prepareStatement(all: Iterable[Model]): ZIO[ZConnection, Throwable, Chunk[Watermark]] = {

      val copies                  = mutable.LinkedHashMap.empty[String, mutable.ListBuffer[String]]
      val watermarks              = ChunkBuilder.make[Watermark]()
      val txs                     = all.onlyTransactions()
      val batchContents           = all.onlyCopies().groupMapReduce(_._table)(_ => 1L)(_ + _)
      def statAttribute(t: Table) = s"pqs.${t.name}.rows_count" -> batchContents.getOrElse(t, 0L)
      all.foreach {
        case c: Copy      => copies.getOrElseUpdate(c._sql, mutable.ListBuffer.empty).addOne(c._row)
        case w: Watermark => watermarks.addOne(w.copy(txSpans = txs.find(_.ix == w.ix).flatMap(_.span).toList))
      }

      val forcedCopies = copies.view
        .map { (sql, rows) => (sql, rows.view.mkString(lineSeparator())) }
        .toSeq
        .sortBy(_._1)
      val copyIO = ZIO.serviceWithZIO[ZConnection](
        _.access { conn =>
          @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
          val api = conn.asInstanceOf[PGRestorableConnection].underlying.asInstanceOf[PGConnection].getCopyAPI
          forcedCopies.foreach { (sql, rows) => api.copyIn(sql, StringReader(rows)) }
        } <* logTrace(
          s"SQL:${lineSeparator()}" +
            s"${forcedCopies.map(x => s"${x._1}${lineSeparator()}${x._2}").mkString(lineSeparator())}"
        )
      )

      val metricsIO = ZIO.foreachDiscard(
        all.view.filter(_.labels.nonEmpty).groupMapReduce(_.labels)(_ => 1)(_ + _)
      )(
        counter.tagged(_).update(_)
      )

      traces.span("execute SQL") {
        copyIO @@ traces.attributes(
          statAttribute(Table.Transactions),
          statAttribute(Table.Events),
          statAttribute(Table.Contracts),
          statAttribute(Table.Exercises)
        )
      } *>
        metricsIO *>
        ZIO.foreachDiscard(txs.flatMap(_.span)) { s =>
          s.linkToCurrentSpan("target" -> "↧ persist to datastore") *>
            s.addEvent("flushed transaction model SQL to datastore")
        } *>
        traces.currentSpan().map { s => watermarks.result().map(_.copy(persistSpans = Seq(s.getSpanContext))) }
    }
  }

  enum Table(val name: String):
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

  final class Transaction(tx: specific.Transaction, val span: Option[DetachedSpan] = None) extends Copy:
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

  extension (models: Iterable[Model])
    def onlyTransactions(): Iterable[Transaction] = models.view.collect { case t: Transaction => t }
    def onlyCopies(): Iterable[Copy]              = models.view.collect { case t: Copy => t }

  extension (tx: Transaction)
    def ifTraced[R, E, A](zio: DetachedSpan => ZIO[R, E, A]) = ZIO.whenCase(tx.span) { case Some(s) => zio(s) }

  // utils

  private[relational] def values: RowValues         = RowValues()
  private given conv: Conversion[RowValues, String] = _.toString
  private[relational] class RowValues {
    private val sb                = StringBuilder()
    override def toString: String = sb.result()
    def apply[A](value: A)(using conv: ValueConverter[A]): RowValues =
      if sb.nonEmpty then sb.append("\t")
      sb.append(conv.convert(value))
      this
  }

  // https://www.postgresql.org/docs/current/sql-copy.html
  private val escaper = new LookupTranslator(
    Map(
      "\b" -> "\\b",
      "\f" -> "\\f",
      "\n" -> "\\n",
      "\r" -> "\\r",
      "\t" -> "\\t",
      ""  -> "\\v",
      "\\" -> "\\\\"
    ).asJava
  )

  private[relational] given booleanConverter: ValueConverter[Boolean]       = value => value.toString
  private[relational] given numericConverter[A: Numeric]: ValueConverter[A] = value => value.toString
  private[relational] given stringConverter: ValueConverter[String]         = value => escaper.translate(value)
  private[relational] given contractIdConverter: ValueConverter[ContractId] = value => value
  private[relational] given partyConverter: ValueConverter[Party]           = value => value
  private[relational] given choiceNameConverter: ValueConverter[ChoiceName] = value => escaper.translate(value.toString)
  private[relational] given idConverter: ValueConverter[IdPlaceholder]      = value => value.id.toString
  private[relational] given jsonConverter: ValueConverter[Value]            = value => escaper.translate(value.toString)

  private[relational] given byteArrayConverter: ValueConverter[Array[Byte]] =
    val HEX_DIGITS = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F')
    value =>
      if value.isEmpty then ""
      else
        val sb = StringBuilder()
        sb.append("\\\\x")
        value.foreach(b => sb.append(HEX_DIGITS(b >> 4 & 15)).append(HEX_DIGITS(b & 15)))
        sb.result()

  private[relational] given instantConverter: ValueConverter[Instant] =
    val fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSXX")
    value => value.atZone(ZoneOffset.UTC).format(fmt)

  private[relational] given optionalConverter[A: ValueConverter]: ValueConverter[Option[A]] =
    case Some(value) => implicitly[ValueConverter[A]].convert(value)
    case None        => "\\N"

  private[relational] given iterableConverter[A: ValueConverter]: ValueConverter[Seq[A]] =
    value => value.map(implicitly[ValueConverter[A]].convert).mkString("{", ",", "}")

  enum EventKind:
    case Create; case Exercise; case Archive
  private[relational] given eventKindConverter: ValueConverter[EventKind] =
    case EventKind.Create   => "create"
    case EventKind.Exercise => "exercise"
    case EventKind.Archive  => "archive"

  enum SourceKind:
    case Stream; case AcsSeed; case LedgerReplay; case DocumentBackfill
  private[relational] given sourceKindConverter: ValueConverter[SourceKind] =
    case SourceKind.Stream           => "stream"
    case SourceKind.AcsSeed          => "acs_seed"
    case SourceKind.LedgerReplay     => "ledger_replay"
    case SourceKind.DocumentBackfill => "document_backfill"

  enum ArchiveSource:
    case Native; case ConsumingExercise
  private[relational] given archiveSourceConverter: ValueConverter[ArchiveSource] =
    case ArchiveSource.Native            => "native"
    case ArchiveSource.ConsumingExercise => "consuming_exercise"

  enum VisibilityRole:
    case Signatory; case Observer; case Witness
  private[relational] given visibilityRoleConverter: ValueConverter[VisibilityRole] =
    case VisibilityRole.Signatory => "signatory"
    case VisibilityRole.Observer  => "observer"
    case VisibilityRole.Witness   => "witness"
}
