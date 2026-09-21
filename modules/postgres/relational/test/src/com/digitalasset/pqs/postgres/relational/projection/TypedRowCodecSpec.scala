package com.digitalasset.pqs.postgres.relational.projection

import com.digitalasset.pqs.postgres.backend.encoding.ValueConverter
import com.digitalasset.pqs.postgres.relational.projection.TypedRowCodec.SqlValue
import com.digitalasset.transcode.codec.json.JsonCodec
import com.digitalasset.transcode.schema.*
import ujson.Value
import zio.test.*

import java.time.{Instant, LocalDate, ZonedDateTime}

object TypedRowCodecSpec extends ZIOSpecDefault:
  private def id(pkg: String, ver: String, entity: String) =
    Identifier(PackageId(pkg), PackageName("Finance"), PackageVersion(ver), ModuleName("Main"), EntityName(entity))

  private def tmpl(i: Identifier, fs: Seq[(String, Descriptor)]): Template[Descriptor] =
    Template(i, Descriptor.constructor(i, Descriptor.record(fs)), None, false, Seq.empty, Seq.empty)

  private val fields = Seq(
    "owner"  -> Descriptor.party,
    "amount" -> Descriptor.numeric(2),
    "active" -> Descriptor.bool,
    "status" -> Descriptor.constructor(id("e1", "1.0.0", "Status"), Descriptor.enumeration(Seq("Active", "Frozen"))),
    "when"   -> Descriptor.timestamp,
    "born"   -> Descriptor.date,
    "memo"   -> Descriptor.optional(Descriptor.text)
  )

  private val shape =
    Shape.resolveAll(Dictionary.make(tmpl(id("p1", "1.0.0", "Asset"), fields)))(
      Shape.Lineage("Finance", "Main", "Asset", Shape.EntityKind.Template)
    )

  private val allFields = Seq(
    "i64"    -> Descriptor.int64,
    "num"    -> Descriptor.numeric(4),
    "txt"    -> Descriptor.text,
    "flag"   -> Descriptor.bool,
    "pty"    -> Descriptor.party,
    "cid"    -> Descriptor.contractId(Descriptor.party),
    "dt"     -> Descriptor.date,
    "ts"     -> Descriptor.timestamp,
    "enm"    -> Descriptor.constructor(id("e1", "1.0.0", "E"), Descriptor.enumeration(Seq("A", "B", "C"))),
    "i64opt" -> Descriptor.optional(Descriptor.int64),
    "txtopt" -> Descriptor.optional(Descriptor.text),
    "dtopt"  -> Descriptor.optional(Descriptor.date)
  )
  private val allId    = id("all", "1.0.0", "All")
  private val allDict  = Dictionary.make(tmpl(allId, allFields))
  private val allShape = Shape.resolveAll(allDict)(Shape.Lineage("Finance", "Main", "All", Shape.EntityKind.Template))
  private val allCodec = DescriptorSchemaProcessor.assertProcess(allDict, JsonCodec())

  private val genText =
    Gen.oneOf(Gen.const("a\tb\nc\\d\"e'f"), Gen.const(""), Gen.const("Ünîçødé ☃"), Gen.alphaNumericString)

  private def genRecord: Gen[Any, DynamicValue] =
    val gens: Seq[Gen[Any, DynamicValue]] = Seq(
      Gen.oneOf(Gen.const(Long.MinValue), Gen.const(Long.MaxValue), Gen.const(0L), Gen.long).map(DynamicValue.Int64(_)),
      Gen.long(-99999999L, 99999999L).map(v => DynamicValue.Numeric(BigDecimal(BigInt(v), 4).toString)),
      genText.map(DynamicValue.Text(_)),
      Gen.boolean.map(DynamicValue.Bool(_)),
      genText.map(DynamicValue.Party(_)),
      genText.map(DynamicValue.ContractId(_)),
      Gen.int(-719162, 2932896).map(DynamicValue.Date(_)),
      Gen.long(-30610224000000000L, 32503680000000000L).map(DynamicValue.Timestamp(_)),
      Gen.int(0, 2).map(DynamicValue.Enumeration(_)),
      Gen.option(Gen.long).map(o => DynamicValue.Optional(o.map(DynamicValue.Int64(_)))),
      Gen.option(genText).map(o => DynamicValue.Optional(o.map(DynamicValue.Text(_)))),
      Gen.option(Gen.int(-719162, 2932896)).map(o => DynamicValue.Optional(o.map(DynamicValue.Date(_))))
    )
    Gen.collectAll(gens).map(vs => DynamicValue.Record(vs*))

  private def sqlNorm(sql: SqlValue): String = sql match
    case SqlValue.Null           => "∅"
    case SqlValue.Bigint(v)      => v.toString
    case SqlValue.Numeric(v)     => v
    case SqlValue.Text(v)        => v
    case SqlValue.Bool(v)        => v.toString
    case SqlValue.Date(v)        => v.toString
    case SqlValue.Timestamptz(v) => v.toString

  private def jsonNorm(json: Value, sql: SqlValue): String = (sql, json) match
    case (_, ujson.Null)                         => "∅"
    case (SqlValue.Date(_), ujson.Str(s))        => LocalDate.parse(s).toString
    case (SqlValue.Timestamptz(_), ujson.Str(s)) => ZonedDateTime.parse(s).toInstant.toString
    case (_, ujson.Str(s))                       => s
    case (_, ujson.Bool(b))                      => b.toString
    case (_, other)                              => other.render()

  def spec = suite("typed row codec")(
    test("decodes each promoted column positionally with enum name, optional and typed values"):
      val record = DynamicValue.Record(
        DynamicValue.Party("Alice"),
        DynamicValue.Numeric("12.50"),
        DynamicValue.Bool(true),
        DynamicValue.Enumeration(1),
        DynamicValue.Timestamp(1600000000000000L),
        DynamicValue.Date(19000),
        DynamicValue.Optional(Some(DynamicValue.Text("hi")))
      )
      assertTrue(
        TypedRowCodec.extract(shape, record) == Seq(
          SqlValue.Text("Alice"),
          SqlValue.Numeric("12.50"),
          SqlValue.Bool(true),
          SqlValue.Text("Frozen"),
          SqlValue.Timestamptz(Instant.ofEpochSecond(1600000000L)),
          SqlValue.Date(LocalDate.ofEpochDay(19000L)),
          SqlValue.Text("hi")
        )
      )
    ,
    test("pads a shorter (older-version) record so a trailing field extracts as SQL NULL"):
      val short = DynamicValue.Record(
        DynamicValue.Party("Bob"),
        DynamicValue.Numeric("0.00"),
        DynamicValue.Bool(false),
        DynamicValue.Enumeration(0),
        DynamicValue.Timestamp(0L),
        DynamicValue.Date(0)
      )
      assertTrue(
        TypedRowCodec.extract(shape, short)(6) == SqlValue.Null,
        TypedRowCodec.extract(shape, short)(3) == SqlValue.Text("Active")
      )
    ,
    test("encodes SqlValues to COPY tokens with the null sentinel and escaped text"):
      val conv = summon[ValueConverter[SqlValue]]
      assertTrue(
        conv.convert(SqlValue.Null) == "\\N",
        conv.convert(SqlValue.Bigint(42L)) == "42",
        conv.convert(SqlValue.Numeric("12.50")) == "12.50",
        conv.convert(SqlValue.Bool(true)) == "true",
        conv.convert(SqlValue.Date(LocalDate.of(2020, 1, 15))) == "2020-01-15",
        conv.convert(SqlValue.Text("a\tb")) == "a\\tb"
      )
    ,
    test("typed extraction agrees with the JSON codec at every promoted field, across types and boundaries"):
      check(genRecord) { record =>
        val json     = allCodec.template(allId).fromDynamicValue(record)
        val pairs    = allShape.promoted.zip(TypedRowCodec.extract(allShape, record))
        val actual   = pairs.map((_, sql) => sqlNorm(sql))
        val expected = pairs.map((f, sql) => jsonNorm(json(f.name), sql))
        assertTrue(actual == expected)
      }
    ,
    test("padding keeps JSON-codec agreement for an older-version row"):
      val v1Id    = id("p1", "1.0.0", "Pv")
      val prefix  = Seq("owner" -> Descriptor.party, "amount" -> Descriptor.numeric(2))
      val evolved = prefix :+ ("memo" -> Descriptor.optional(Descriptor.text))
      val dict    = Dictionary.make(tmpl(v1Id, prefix), tmpl(id("p2", "2.0.0", "Pv"), evolved))
      val sh      = Shape.resolveAll(dict)(Shape.Lineage("Finance", "Main", "Pv", Shape.EntityKind.Template))
      val codec   = DescriptorSchemaProcessor.assertProcess(dict, JsonCodec())
      val v1Row   = DynamicValue.Record(DynamicValue.Party("Alice"), DynamicValue.Numeric("1.00"))
      val json    = codec.template(v1Id).fromDynamicValue(v1Row)
      val row     = TypedRowCodec.extract(sh, v1Row)
      assertTrue(
        sqlNorm(row(0)) == jsonNorm(json("owner"), row(0)),
        sqlNorm(row(1)) == jsonNorm(json("amount"), row(1)),
        row(2) == SqlValue.Null
      )
  )
end TypedRowCodecSpec
