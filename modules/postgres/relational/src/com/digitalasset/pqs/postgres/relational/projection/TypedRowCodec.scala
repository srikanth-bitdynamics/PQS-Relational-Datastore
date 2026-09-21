package com.digitalasset.pqs.postgres.relational.projection

import com.digitalasset.pqs.postgres.backend.encoding.{ValueConverter, given}
import com.digitalasset.transcode.schema.DynamicValue

import java.time.{Instant, LocalDate}

object TypedRowCodec:
  enum SqlValue:
    case Null
    case Bigint(v: Long)
    case Numeric(v: String)
    case Text(v: String)
    case Bool(v: Boolean)
    case Date(v: LocalDate)
    case Timestamptz(v: Instant)

  given ValueConverter[LocalDate] = _.toString

  given ValueConverter[SqlValue] = sv =>
    sv match
      case SqlValue.Null           => "\\N"
      case SqlValue.Bigint(v)      => v.toString
      case SqlValue.Numeric(v)     => v
      case SqlValue.Text(v)        => summon[ValueConverter[String]].convert(v)
      case SqlValue.Bool(v)        => v.toString
      case SqlValue.Date(v)        => summon[ValueConverter[LocalDate]].convert(v)
      case SqlValue.Timestamptz(v) => summon[ValueConverter[Instant]].convert(v)

  def extract(shape: Shape.ResolvedShape, record: DynamicValue): Seq[SqlValue] =
    val cells = record.recordIteratorPadded(shape.unionFieldCount).toVector
    shape.promoted.map { f =>
      val cell = cells(f.position)
      if f.nullable then cell.optional.fold(SqlValue.Null)(decode(f, _))
      else decode(f, cell)
    }

  private def decode(f: Shape.PromotedField, dv: DynamicValue): SqlValue =
    f.pgType match
      case Shape.PgType.Bigint      => SqlValue.Bigint(dv.int64)
      case _: Shape.PgType.Numeric  => SqlValue.Numeric(dv.numeric)
      case Shape.PgType.Date        => SqlValue.Date(LocalDate.ofEpochDay(dv.date.toLong))
      case Shape.PgType.Timestamptz => SqlValue.Timestamptz(microsToInstant(dv.timestamp))
      case Shape.PgType.Bool        => SqlValue.Bool(dv.bool)
      case Shape.PgType.Text => f.enumCases.fold(SqlValue.Text(dv.text))(cases => SqlValue.Text(cases(dv.enumeration)))

  private def microsToInstant(micros: Long): Instant =
    Instant.ofEpochSecond(micros / 1000000L, (micros % 1000000L) * 1000L)
end TypedRowCodec
