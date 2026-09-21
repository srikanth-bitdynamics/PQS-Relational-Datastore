package com.digitalasset.pqs.postgres.relational

import com.digitalasset.auth
import com.digitalasset.auth.{Auth, TokenService}
import com.digitalasset.canonical.{ContractFilter, MetadataFilter, given}
import com.digitalasset.pqs.app.*
import com.digitalasset.pqs.logging.FileLogging
import com.digitalasset.pqs.postgres.backend
import com.digitalasset.transcode.schema.IdentifierFilter
import com.digitalasset.zio.daml
import com.digitalasset.zio.daml.{DamlSchema, LedgerScope}
import com.digitalasset.zio.daml.ledgerapi.PackageService
import zio.Console.printLine
import zio.ZIO.{logInfo, logTrace, serviceWithZIO}
import com.digitalasset.pqs.postgres.relational.projection.ProjectionRegistry
import zio.config.magnolia.{Descriptor, describe}
import zio.jdbc.{ZConnection, transaction}
import zio.{ZIO, ZLayer}

object Main extends ComposableApp:
  def app =
    "postgres-relational"
      @@ Command("Perform operations supporting Postgres database (w/ relational payload representation)")
      - (appSchema | appProjection)

  private def appSchema =
    "schema" @@ Command("Infer or apply database schema derived from Daml package metadata")
      - (appSchemaApply | appSchemaShow)

  private def appSchemaApply = (
    "apply" @@ Command("Infer required database schema, apply it to data store and quit")
      - cliConfig[ConfigSchemaApply]
      `map` applySchema
  )

  private def appSchemaShow = (
    "show" @@ Command("Infer required database schema, display it and quit")
      - cliConfig[ConfigSchemaShow]
      `map` showSchema
  )

  private def applySchema(config: ZLayer[Any, Throwable, ConfigSchemaApply]) =
    (for
      _          <- logInfo("Applying required datastore schema")
      config     <- ZIO.service[backend.SchemaConfig]
      poolConfig <- ZIO.service[backend.PostgresConfig]
      instanceId <- ZIO.service[backend.InstanceId]
      _          <- RelationalSchema.applySchema(poolConfig, instanceId, config.baseline)
      _          <- logInfo("Applied required datastore schema")
      _          <- printLine("Finished applying schema to datastore")
    yield ())
      .provide(
        com.digitalasset.pqs.appversion.LogVersion,
        (config.project(_.ledger.auth) ++ config.project(_.oauth)) >>> Auth.live(LedgerScope),
        TokenService.live,
        config.project(_.ledger) >>> daml.Channel.live,
        config.project(_.postgres),
        config.project(_.schema),
        backend.instanceId,
        backend.connectionPool,
        config.project(_.filter.contracts),
        ZLayer.succeed(MetadataFilter(IdentifierFilter.AcceptAll)),
        config.project(_.ledger) >>> PackageService.live >>> DamlSchema.layer,
        DamlSchema.produce(RelSqlSchema)
      )
      .bootstrap(
        config.project(_.logger).orElse(FileLogging.default) >>> com.digitalasset.pqs.cli.bootstrap
      )

  private def showSchema(config: ZLayer[Any, Throwable, ConfigSchemaShow]) =
    serviceWithZIO[RelSqlSchema] { s =>
      logInfo("Displaying required datastore schema") *>
        logTrace(s"Schema:${System.lineSeparator}${s.schema}") *>
        logTrace(s"Mappings:${System.lineSeparator}${s.mappings}") *>
        printVersionHeader *> printLine(s.schema) *> printLine(s.mappings)
    }
      .provide(
        com.digitalasset.pqs.appversion.LogVersion,
        (config.project(_.ledger.auth) ++ config.project(_.oauth)) >>> Auth.live(LedgerScope),
        TokenService.live,
        config.project(_.ledger) >>> daml.Channel.live,
        config.project(_.filter.contracts),
        ZLayer.succeed(MetadataFilter(IdentifierFilter.AcceptAll)),
        config.project(_.ledger) >>> PackageService.live >>> DamlSchema.layer,
        DamlSchema.produce(RelSqlSchema)
      )
      .bootstrap(
        config.project(_.logger).orElse(FileLogging.default) >>> com.digitalasset.pqs.cli.bootstrap
      )

  private def appProjection =
    "projection" @@ Command("Inspect relational typed-column projections")
      - (appProjectionList | appProjectionShow)

  private def appProjectionList = (
    "list" @@ Command("List projection versions and their status")
      - cliConfig[ConfigProjection]
      `map` projectionList
  )

  private def appProjectionShow = (
    "show" @@ Command("Show the definition of the latest projection version")
      - cliConfig[ConfigProjection]
      `map` projectionShow
  )

  private def projectionList(config: ZLayer[Any, Throwable, ConfigProjection]) =
    withRegistry(config)(
      ProjectionRegistry.list.flatMap(rows =>
        ZIO.foreachDiscard(rows)(r => printLine(s"${r.version}\t${r.status}\t${r.hash}"))
      )
    )

  private def projectionShow(config: ZLayer[Any, Throwable, ConfigProjection]) =
    withRegistry(config)(
      ProjectionRegistry.list.flatMap(rows =>
        rows.lastOption.fold(printLine("No projection defined"))(r => printLine(ujson.write(r.definition, indent = 2)))
      )
    )

  private def withRegistry(config: ZLayer[Any, Throwable, ConfigProjection])(op: ZIO[ZConnection, Throwable, Unit]) =
    transaction(op)
      .provide(
        com.digitalasset.pqs.appversion.LogVersion,
        config.project(_.postgres),
        backend.instanceId,
        backend.connectionPool
      )
      .bootstrap(config.project(_.logger).orElse(FileLogging.default) >>> com.digitalasset.pqs.cli.bootstrap)

  private val printVersionHeader = com.digitalasset.pqs.appversion.getVersion.flatMap {
    case (title, version, _) =>
      val content        = s"generated by $title, version: $version"
      val commentOpening = "/" + "*" * (content.length + 4)
      ZIO.foreach(List(commentOpening, " * " + content + " *", " " + commentOpening.reverse))(printLine(_))
  }

  private final case class ConfigSchemaShow(
      ledger: daml.Config,
      oauth: auth.Config.OAuth,
      filter: Filters = Filters(),
      logger: FileLogging.Config
  )

  private final case class ConfigSchemaApply(
      ledger: daml.Config,
      oauth: auth.Config.OAuth,
      postgres: backend.PostgresConfig,
      schema: backend.SchemaConfig,
      filter: Filters = Filters(),
      logger: FileLogging.Config
  )

  private final case class ConfigProjection(
      postgres: backend.PostgresConfig,
      logger: FileLogging.Config
  )

  private final case class Filters(
      @describe("Filter expression determining which templates and interfaces to include")
      contracts: ContractFilter = ContractFilter(IdentifierFilter.AcceptAll)
  )
end Main
