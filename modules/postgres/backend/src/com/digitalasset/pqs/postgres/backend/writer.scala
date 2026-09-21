package com.digitalasset.pqs.postgres.backend

import com.digitalasset.canonical.{ContractId, Party}
import com.digitalasset.canonical.specific.Offset
import com.digitalasset.pqs.o11y.traces
import com.digitalasset.pqs.o11y.traces.{DetachedSpan, given}
import io.opentelemetry.api.trace.SpanContext
import org.apache.commons.text.translate.LookupTranslator
import org.postgresql.PGConnection
import ujson.Value
import zio.ZIO.logTrace
import zio.jdbc.shims.postgres.PGRestorableConnection
import zio.jdbc.{JdbcDecoder, ZConnection}
import zio.metrics.{Metric, MetricLabel}
import zio.{Chunk, ChunkBuilder, ZIO}

import java.io.StringReader
import java.lang.System.lineSeparator
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

object encoding:
  trait ValueConverter[A] { def convert(value: A): String }

  def values: RowValues = RowValues()

  given conv: Conversion[RowValues, String] = _.toString

  final class RowValues:
    private val sb                = StringBuilder()
    override def toString: String = sb.result()
    def apply[A](value: A)(using c: ValueConverter[A]): RowValues =
      if sb.nonEmpty then sb.append("\t")
      sb.append(c.convert(value))
      this

  implicit val offsetEncoder: JdbcDecoder[Offset] = (ix, rs) => (ix, Offset.Absolute(rs.getLong(ix)))

  extension (offset: Offset) def toSqlValue: Long = offset.toLongOffset

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

  def escape(value: String): String = escaper.translate(value)

  given booleanConverter: ValueConverter[Boolean]       = value => value.toString
  given numericConverter[A: Numeric]: ValueConverter[A] = value => value.toString
  given stringConverter: ValueConverter[String]         = value => escape(value)
  given contractIdConverter: ValueConverter[ContractId] = value => value
  given partyConverter: ValueConverter[Party]           = value => value
  given idConverter: ValueConverter[IdPlaceholder]      = value => value.id.toString
  given jsonConverter: ValueConverter[Value]            = value => escape(value.toString)

  given byteArrayConverter: ValueConverter[Array[Byte]] =
    val HEX_DIGITS = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F')
    value =>
      if value.isEmpty then ""
      else
        val sb = StringBuilder()
        sb.append("\\\\x")
        value.foreach(b => sb.append(HEX_DIGITS(b >> 4 & 15)).append(HEX_DIGITS(b & 15)))
        sb.result()

  given instantConverter: ValueConverter[Instant] =
    val fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSXX")
    value => value.atZone(ZoneOffset.UTC).format(fmt)

  given optionalConverter[A: ValueConverter]: ValueConverter[Option[A]] =
    case Some(value) => summon[ValueConverter[A]].convert(value)
    case None        => "\\N"

  given iterableConverter[A: ValueConverter]: ValueConverter[Seq[A]] =
    value => value.map(summon[ValueConverter[A]].convert).mkString("{", ",", "}")
end encoding

object copy:
  trait CopyTable { def name: String }

  trait Model { def labels: Set[MetricLabel] }
  trait Copy extends Model:
    def _table: CopyTable
    def _sql: String
    def _row: String
  trait TransactionCopy extends Copy:
    def ix: Long
    def offset: Offset
    def span: Option[DetachedSpan]

  final case class Watermark(
      ix: Long,
      offset: Offset,
      seenAts: Seq[Long],
      txSpans: Seq[DetachedSpan] = Seq.empty,
      persistSpans: Seq[SpanContext] = Seq.empty
  ) extends Model:
    val labels = l("type" -> "watermark")

  given watermarkOrdering: Ordering[Watermark] = Ordering.by(_.ix)

  def l(kv: (String, Any)*): Set[MetricLabel] = kv.map((k, v) => MetricLabel(k, v.toString)).toSet

  extension (models: Iterable[Model])
    def onlyTransactions(): Iterable[TransactionCopy] = models.view.collect { case t: TransactionCopy => t }
    def onlyCopies(): Iterable[Copy]                  = models.view.collect { case t: Copy => t }

  extension (tx: TransactionCopy)
    def ifTraced[R, E, A](zio: DetachedSpan => ZIO[R, E, A]) = ZIO.whenCase(tx.span) { case Some(s) => zio(s) }

  object Model:
    private val counter = Metric.counter("pipeline_events", "Processed ledger events")

    def prepareStatement(
        all: Iterable[Model],
        statTables: Seq[CopyTable]
    ): ZIO[ZConnection, Throwable, Chunk[Watermark]] =
      val copies                      = mutable.LinkedHashMap.empty[String, mutable.ListBuffer[String]]
      val watermarks                  = ChunkBuilder.make[Watermark]()
      val txs                         = all.onlyTransactions()
      val batchContents               = all.onlyCopies().groupMapReduce(_._table)(_ => 1L)(_ + _)
      def statAttribute(t: CopyTable) = s"pqs.${t.name}.rows_count" -> batchContents.getOrElse(t, 0L)
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
        copyIO @@ traces.attributes(statTables.map(statAttribute)*)
      } *>
        metricsIO *>
        ZIO.foreachDiscard(txs.flatMap(_.span)) { s =>
          s.linkToCurrentSpan("target" -> "↧ persist to datastore") *>
            s.addEvent("flushed transaction model SQL to datastore")
        } *>
        traces.currentSpan().map { s => watermarks.result().map(_.copy(persistSpans = Seq(s.getSpanContext))) }
  end Model
end copy
