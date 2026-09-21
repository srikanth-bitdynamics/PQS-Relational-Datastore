package com.digitalasset.pqs.postgres.relational.projection

import com.digitalasset.pqs.postgres.backend.encoding.ValueConverter
import com.digitalasset.pqs.postgres.relational.projection.TypedRowCodec.SqlValue
import com.digitalasset.pqs.postgres.relational.projection.TypedRowCodec.given
import zio.test.*

import java.time.Instant

object TimestampPrecisionSpec extends ZIOSpecDefault:
  def spec = suite("timestamp precision")(
    test("a microsecond timestamp keeps all six fractional digits in the COPY token"):
      val token = summon[ValueConverter[SqlValue]].convert(SqlValue.Timestamptz(Instant.ofEpochSecond(0L, 123456000L)))
      assertTrue(token.contains(".123456"))
  )
end TimestampPrecisionSpec
