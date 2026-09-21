package com.digitalasset.pqs.postgres.relational.projection

import com.digitalasset.transcode.Codec
import com.digitalasset.transcode.schema.*
import ujson.Value
import zio.ZIO
import zio.jdbc.*

object ProjectionBackfill:
  def run(
      codec: Dictionary[Codec[Value]],
      shapes: Map[String, Shape.ResolvedShape]
  ): ZIO[ZConnection, Throwable, Long] =
    for
      through <- sql"select tx_ix from latest_checkpoint()".query[Long].selectOne.map(_.getOrElse(0L))
      _       <- ZIO.foreachDiscard(shapes.values.toSeq)(shape => backfillLineage(codec, shape, through))
    yield through

  private def backfillLineage(
      codec: Dictionary[Codec[Value]],
      shape: Shape.ResolvedShape,
      through: Long
  ): ZIO[ZConnection, Throwable, Unit] =
    if shape.promoted.isEmpty then ZIO.unit
    else
      val l = shape.lineage
      sql"""select base_table from __rel_entity
            where package_name = ${l.packageName} and module_name = ${l.moduleName}
              and entity_name = ${l.entityName} and kind = 'template'"""
        .query[String]
        .selectOne
        .flatMap {
          case None => ZIO.unit
          case Some(tbl) =>
            val select = SqlFragment(
              s"""select p.contract_pk, pkg.id, pkg.name, pkg.version, p.payload_json::text
                  from ${tbl} p
                  join __rel_contracts c on c.contract_pk = p.contract_pk
                  join __rel_package pkg on pkg.id = c.representative_package_id
                  where c.redaction_id is null and c.created_tx_ix <= """
            ) ++ sql"$through"
            select
              .query[(Long, String, String, String, String)]
              .selectAll
              .flatMap(rows => ZIO.foreachDiscard(rows)(row => backfillRow(codec, shape, tbl, row)))
        }

  private def backfillRow(
      codec: Dictionary[Codec[Value]],
      shape: Shape.ResolvedShape,
      tbl: String,
      row: (Long, String, String, String, String)
  ): ZIO[ZConnection, Throwable, Unit] =
    val (contractPk, packageId, packageName, packageVersion, json) = row
    val id = Identifier(
      PackageId(packageId),
      PackageName(packageName),
      PackageVersion(packageVersion),
      ModuleName(shape.lineage.moduleName),
      EntityName(shape.lineage.entityName)
    )
    val dv     = codec.template(id).toDynamicValue(ujson.read(json))
    val values = shape.promoted.map(_.name).zip(TypedRowCodec.extract(shape, dv))
    val assigns =
      values.map((name, value) => SqlFragment(quoteIdent(name)) ++ sql" = " ++ bind(value)).mkFragment(sql", ")
    (SqlFragment(s"update ${tbl} set ") ++ assigns ++ sql" where contract_pk = $contractPk").update.unit

  private def bind(value: TypedRowCodec.SqlValue): SqlFragment =
    value match
      case TypedRowCodec.SqlValue.Null           => sql"null"
      case TypedRowCodec.SqlValue.Bigint(v)      => sql"$v"
      case TypedRowCodec.SqlValue.Numeric(v)     => sql"${v}::numeric"
      case TypedRowCodec.SqlValue.Text(v)        => sql"$v"
      case TypedRowCodec.SqlValue.Bool(v)        => sql"$v"
      case TypedRowCodec.SqlValue.Date(v)        => sql"${v.toString}::date"
      case TypedRowCodec.SqlValue.Timestamptz(v) => sql"${v.toString}::timestamptz"

  private def quoteIdent(name: String): String = "\"" + name.replace("\"", "\"\"") + "\""
end ProjectionBackfill
