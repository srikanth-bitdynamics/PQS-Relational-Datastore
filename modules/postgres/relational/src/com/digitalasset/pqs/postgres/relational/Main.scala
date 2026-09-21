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
import com.digitalasset.pqs.postgres.relational.projection.{
  ProjectionApply,
  ProjectionBackfill,
  ProjectionBinding,
  ProjectionDefinition,
  ProjectionQuery,
  ProjectionRegistry,
  Shape
}
import com.digitalasset.transcode.Codec
import com.digitalasset.transcode.codec.json.JsonCodec
import com.digitalasset.transcode.schema.{Dictionary, Schema}
import ujson.Value
import zio.config.magnolia.{Descriptor, describe}
import zio.jdbc.*
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
      pool       <- ZIO.service[ZConnectionPool]
      _ <- WriterFence.requireIdle(pool, poolConfig.maxConnections)(
        RelationalSchema.applySchema(poolConfig, instanceId, config.baseline)
      )
      _ <- logInfo("Applied required datastore schema")
      _ <- printLine("Finished applying schema to datastore")
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
    "projection" @@ Command("Manage relational typed-column projections")
      - (appProjectionApply | appProjectionBackfill | appProjectionActivate | appProjectionList | appProjectionShow)

  private def appProjectionApply = (
    "apply" @@ Command("Resolve the configured projection, add typed columns and record a draft version")
      - cliConfig[ConfigProjectionApply]
      `map` projectionApply
  )

  private def appProjectionBackfill = (
    "backfill" @@ Command("Populate typed columns of the latest draft projection from stored payloads")
      - cliConfig[ConfigProjectionBackfill]
      `map` projectionBackfill
  )

  private def appProjectionActivate = (
    "activate" @@ Command("Publish the latest draft projection once it is backfilled through the watermark")
      - cliConfig[ConfigProjection]
      `map` projectionActivate
  )

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

  private def projectionApply(config: ZLayer[Any, Throwable, ConfigProjectionApply]) =
    (for
      schema      <- ZIO.service[Schema]
      projections <- ZIO.service[Map[String, ProjectionConfig]]
      outcome     <- transaction(ProjectionApply.apply(projections.view.mapValues(_.toDefinition).toMap, schema))
      _           <- printLine(ProjectionApply.render(outcome))
    yield ())
      .provide(
        com.digitalasset.pqs.appversion.LogVersion,
        (config.project(_.ledger.auth) ++ config.project(_.oauth)) >>> Auth.live(LedgerScope),
        TokenService.live,
        config.project(_.ledger) >>> daml.Channel.live,
        config.project(_.postgres),
        config.project(_.projections),
        backend.instanceId,
        backend.connectionPool,
        config.project(_.ledger) >>> PackageService.live >>> ZLayer.fromZIO(serviceWithZIO[PackageService](_.getSchema))
      )
      .bootstrap(config.project(_.logger).orElse(FileLogging.default) >>> com.digitalasset.pqs.cli.bootstrap)

  private def projectionBackfill(config: ZLayer[Any, Throwable, ConfigProjectionBackfill]) =
    (for
      codec <- ZIO.service[Dictionary[Codec[Value]]]
      message <- transaction(
        ProjectionRegistry.latestDraft.flatMap {
          case None => ZIO.succeed("No draft projection to backfill")
          case Some(version) =>
            for
              shapeJson <- ProjectionRegistry.resolvedShapeOf(version)
              shapes = shapeJson.fold(Map.empty[String, Shape.ResolvedShape])(ProjectionBinding.parse)
              through <- ProjectionBackfill.run(codec, shapes, version)
              _       <- ProjectionRegistry.setBackfilledThrough(version, through)
            yield s"Backfilled projection version $version through tx_ix $through"
        }
      )
      _ <- printLine(message)
    yield ())
      .provide(
        com.digitalasset.pqs.appversion.LogVersion,
        (config.project(_.ledger.auth) ++ config.project(_.oauth)) >>> Auth.live(LedgerScope),
        TokenService.live,
        config.project(_.ledger) >>> daml.Channel.live,
        config.project(_.postgres),
        backend.instanceId,
        backend.connectionPool,
        ZLayer.succeed[ContractFilter](ContractFilter(IdentifierFilter.AcceptAll)),
        ZLayer.succeed(MetadataFilter(IdentifierFilter.AcceptAll)),
        config.project(_.ledger) >>> PackageService.live >>> DamlSchema.layer,
        DamlSchema.produce(JsonCodec()).update(_.matchByPackageId)
      )
      .bootstrap(config.project(_.logger).orElse(FileLogging.default) >>> com.digitalasset.pqs.cli.bootstrap)

  private def projectionActivate(config: ZLayer[Any, Throwable, ConfigProjection]) =
    withRegistry(config)(
      ProjectionRegistry.latestDraft.flatMap {
        case None => printLine("No draft projection to activate")
        case Some(version) =>
          for
            row       <- ProjectionRegistry.get(version)
            watermark <- sql"select tx_ix from latest_checkpoint()".query[Long].selectOne.map(_.getOrElse(0L))
            _ <- row.flatMap(_.backfilledThroughIx) match
              case Some(through) if through >= watermark =>
                ProjectionRegistry.activate(version) *> printLine(s"Activated projection version $version")
              case _ =>
                printLine(
                  s"Projection version $version is not backfilled through the current watermark ($watermark); run backfill first"
                )
          yield ()
      }
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

  private final case class ProjectionQueryConfig(
      filter: List[String] = List.empty,
      order: List[String] = List.empty
  )
  private object ProjectionQueryConfig:
    given Descriptor[ProjectionQueryConfig] = Descriptor.derived

  private final case class ProjectionConfig(
      templates: List[String] = List.empty,
      promote: List[String] = List.empty,
      queries: List[ProjectionQueryConfig] = List.empty
  ):
    def toDefinition: ProjectionDefinition =
      ProjectionDefinition(templates, promote, queries.map(q => ProjectionQuery(q.filter, q.order)))
  private object ProjectionConfig:
    given Descriptor[ProjectionConfig] = Descriptor.derived

  private final case class ConfigProjectionApply(
      ledger: daml.Config,
      oauth: auth.Config.OAuth,
      postgres: backend.PostgresConfig,
      projections: Map[String, ProjectionConfig] = Map.empty,
      logger: FileLogging.Config
  )

  private final case class ConfigProjectionBackfill(
      ledger: daml.Config,
      oauth: auth.Config.OAuth,
      postgres: backend.PostgresConfig,
      logger: FileLogging.Config
  )

  private final case class Filters(
      @describe("Filter expression determining which templates and interfaces to include")
      contracts: ContractFilter = ContractFilter(IdentifierFilter.AcceptAll)
  )
end Main
