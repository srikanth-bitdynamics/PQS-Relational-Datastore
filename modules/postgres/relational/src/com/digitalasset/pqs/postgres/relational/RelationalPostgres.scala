package com.digitalasset.pqs.postgres.relational

import com.digitalasset.canonical
import com.digitalasset.canonical.{ContractId, Party, UserRight}
import com.digitalasset.canonical.specific.{Offset, ReassignmentEvent, TreeEvent}
import com.digitalasset.pqs.backend.Datastore
import com.digitalasset.pqs.o11y.metrics.latency
import com.digitalasset.pqs.o11y.traces
import com.digitalasset.pqs.o11y.traces.given
import com.digitalasset.pqs.postgres.backend.*
import com.digitalasset.pqs.postgres.relational.projection.{ProjectionBinding, Shape, TypedRowCodec}
import com.digitalasset.transcode.Codec
import com.digitalasset.transcode.schema.{ChoiceName, Dictionary, DynamicValue, Identifier}
import ujson.Value
import zio.ZIO.{logDebug, logInfo}
import zio.jdbc.*
import zio.metrics.Metric
import zio.metrics.MetricKeyType.Histogram.Boundaries
import zio.stream.{ZChannel, ZPipeline, ZSink}
import zio.{Chunk, ChunkBuilder, Schedule, ZEnvironment, ZIO, ZLayer, durationInt}

import scala.collection.mutable
import scala.language.implicitConversions

final case class RelationalPostgres(
    config: SchemaConfig,
    poolConfig: PostgresConfig,
    pool: ZConnectionPool,
    schema: RelSqlSchema,
    codec: Dictionary[Codec[Value]],
    getEntityPk: Identifier => specific.EntityTypePk,
    getExercisePk: (Identifier, ChoiceName) => specific.EntityTypePk,
    getBaseTable: Identifier => String,
    getViewTable: Identifier => String,
    isTemplate: Identifier => Boolean,
    placeholders: IdPlaceholder.Factory,
    projectionShapes: Map[String, Shape.ResolvedShape]
) extends Datastore:
  import com.digitalasset.pqs.postgres.relational.model.{offsetEncoder, toSqlValue}

  private def promotedFor(id: Identifier, payload: DynamicValue): Seq[(String, TypedRowCodec.SqlValue)] =
    projectionShapes.get(s"${id.packageName}:${id.moduleName}:${id.entityName}") match
      case Some(shape) => shape.promoted.map(_.name).zip(TypedRowCodec.extract(shape, payload))
      case None        => Seq.empty

  private val Genesis: Datastore.Checkpoint = (Offset.Genesis, 0L)
  private val env                           = ZEnvironment(pool) ++ ZEnvironment(config) ++ ZEnvironment(poolConfig)
  private val tx                            = ZLayer.succeedEnvironment(env) >>> transaction
  private val BatchEntitiesThreshold        = 10_000
  private val BatchReleaseWindow            = 200.millis
  private val ReservedConnections           = 1
  private val IngestParallelism             = math.max(1, poolConfig.maxConnections - ReservedConnections)

  override def capabilities = Datastore.Capabilities(reassignments = false, coverage = true)

  override def registerActiveWriterAndCleanupTransactions = tx(
    sql"call __rel_cleanup_transactions_after_watermark()".execute
  )

  override def getFirstCheckpoint = tx(
    sql"select ledger_offset, tx_ix from oldest_checkpoint()".query[Datastore.Checkpoint].selectOne.someOrElse(Genesis)
  )

  override def getLastCheckpoint = tx(
    sql"select ledger_offset, tx_ix from latest_checkpoint()".query[Datastore.Checkpoint].selectOne.someOrElse(Genesis)
  )

  override def recordCoverage(record: Datastore.CoverageRecord) =
    val (allParties, parties) = record.rights match
      case UserRight.AsAnyParty    => (true, Option.empty[String])
      case UserRight.AsParties(ps) => (false, Some(ps.map(p => s""""$p"""").mkString("{", ",", "}")))
    val treeStream = record.datasource match
      case Datastore.Datasource.TransactionTreeStream => true
      case Datastore.Datasource.TransactionStream     => false
    tx(
      sql"call __rel_ensure_writer_valid()".execute *>
        sql"""insert into __query_coverage (
                instance_id, source_kind, requested_from_offset, actual_from_offset, through_offset, source_pruned_offset,
                acs_seed_offset, ingested_all_parties, ingested_parties, contract_filter, metadata_filter, tree_stream,
                create_history_complete, exercise_history_complete, archive_history_complete, archive_visibility_complete,
                reassignment_history_complete, assignment_origin_state_complete, started_at, completed_at)
              values (
                current_setting('scribe.instance'),
                ${"stream"}::rel_source_kind, ${record.normalizedStart.toSqlValue}, ${record.actualStart.toSqlValue},
                ${record.actualStart.toSqlValue}, ${Option.empty[Long]},
                (select ledger_offset from __rel_transactions where tx_ix = 0),
                $allParties, ${parties}::text[], ${record.contractFilter}, ${record.metadataFilter}, $treeStream,
                true, $treeStream, true, true, false, false, now(), null)""".update.unit
    )

  override def processAcs = (
    waitPoint("pipeline_wp_acs_events", poolConfig.bufferSize, 1024 min poolConfig.bufferSize)
      >>> convertAcsEventsToStatements
      >>> waitPoint("pipeline_wp_acs_statements", poolConfig.bufferSize, 1024 min poolConfig.bufferSize)
      >>> batchStatements
      >>> waitPoint("pipeline_wp_acs_batched_statements")
      >>> prepareStatements
      >>> waitPoint("pipeline_wp_acs_prepared_statements")
      >>> executePar(IngestParallelism)
      >>> ZPipeline.flattenChunks
      >>> updateAcsOffsets
      >>> handleWatermarks
      >>> ZSink.drain
  ).provideEnvironment(env)

  override def processTransactions = (
    waitPoint("pipeline_wp_events", poolConfig.bufferSize, 1024 min poolConfig.bufferSize)
      >>> convertTransactionEventsToStatements(8)
      >>> waitPoint("pipeline_wp_statements", poolConfig.bufferSize, 1024 min poolConfig.bufferSize)
      >>> batchStatements
      >>> waitPoint("pipeline_wp_batched_statements")
      >>> prepareStatements
      >>> waitPoint("pipeline_wp_prepared_statements")
      >>> executeParUnordered(IngestParallelism)
      >>> waitPoint("pipeline_wp_watermarks", 1024)
      >>> reorderCheckpoints
      >>> handleWatermarks
      >>> ZSink.drain
  ).provideEnvironment(env)

  // privates

  private def convertAcsEventsToStatements =
    val trackConvert = latency("pipeline_convert_acs_event", "Latency of converting ACS events")
    ZPipeline[canonical.specific.Event.Created | Offset]
      .mapChunksZIO(chunk =>
        ZIO.whenCase(chunk.headOption) {
          case Some(Offset.Genesis) =>
            tx(
              model.Model.prepareStatement(
                Chunk(
                  model.Transaction(
                    specific.Transaction(Genesis._2, Offset.Genesis, None, None, None, None, None, None)
                  )
                ),
                model.statTables
              )
            ).as(Chunk.empty)
        } *> ZIO.attempt {
          chunk.collect {
            case evt: canonical.specific.Event.Created => insertEvent(Genesis._2, model.SourceKind.AcsSeed, None, evt)
            case offset: Offset.Absolute               => Chunk(model.Watermark(Genesis._2, offset, Seq.empty))
          }
        } @@ trackConvert
      )
      .tap(x => logDebug(s"Converted ${x.length} ACS events to SQL fragments"))

  private def convertTransactionEventsToStatements(n: Int) =
    val trackConvert = latency("pipeline_convert_transaction", "Latency of converting transactions")
    type TX = (
        canonical.specific.Transaction[canonical.specific.Event | TreeEvent | ReassignmentEvent],
        Datastore.TransactionIndex
    )
    ZPipeline
      .fromChannel(
        ZChannel
          .identity[Throwable, Chunk[TX], Any]
          .mapOutZIOPar(n)(chunk =>
            chunk.mapZIO { (tx, ix) =>
              for
                _      <- tx.span.addEvent("converting canonical transaction to domain model")
                result <- ZIO.attempt { convertTransactionToSqlStatements(tx, ix) } @@ trackConvert
                _      <- tx.span.addEvent("converted canonical transaction to domain model")
              yield result
            }
          )
      )
      .tap(x => logDebug(s"Converted ${x.length} transaction events to SQL fragments"))

  private def batchStatements =
    ZPipeline[Chunk[model.Model]]
      .aggregateAsyncWithin(
        ZSink.foldChunks(
          ChunkBuilder.make[model.Model]() -> 0
        ) { (acc, size) =>
          size < BatchEntitiesThreshold
        } {
          case ((acc, size), in) =>
            var s = size
            for chunk <- in; elem <- chunk do { acc.addOne(elem); s += 1 }
            (acc, s)
        },
        Schedule.spaced(BatchReleaseWindow)
      )
      .map(_._1.result())
      .tap { models =>
        ZIO.foreachDiscard(models.onlyTransactions())(_.ifTraced(_.addEvent("released transaction model into batch")))
      }
      .tap(x => logDebug(s"Aggregated ${x.length} SQL fragments into single batch"))

  private def prepareStatements =
    val trackPrepare = latency("pipeline_prepare_batch_latency", "Latency of preparing batches of statements")
    val trackExecute = latency("pipeline_execute_batch_latency", "Latency of executing batches of statements")
    ZPipeline[Chunk[model.Model]].mapChunksZIO { chunk =>
      ZIO.foreach(chunk) { models =>
        val onlyTxs = models.onlyTransactions()
        ZIO.attempt {
          traces.span("execute batch") {
            (sql"call __rel_ensure_writer_valid()".execute *> model.Model.prepareStatement(models, model.statTables))
              @@ trackExecute
              @@ traces.attributes("pqs.batch.models_count" -> models.length.toLong)
              <* ZIO.foreachDiscard(onlyTxs) { tx =>
                tx.ifTraced(
                  _.linkFromCurrentSpan(
                    "target" -> "↥ incoming transaction",
                    "offset" -> (tx.offset.toSqlValue)
                  )
                )
              }
          }
        } <* ZIO.foreachDiscard(onlyTxs)(_.ifTraced(_.addEvent("prepared SQL statements for transaction model")))
      } @@ trackPrepare
    }

  private def reorderCheckpoints =
    type AccumulatorChannel =
      ZChannel[Any, Nothing, Chunk[Chunk[model.Watermark]], Any, Nothing, Chunk[model.Watermark], Unit]
    def accumulator(state: mutable.ArrayBuffer[model.Watermark]): AccumulatorChannel = ZChannel.readWithCause(
      in => {
        for chunk <- in do state.addAll(chunk)
        state.sortInPlace()
        val consecutive = (state.view zip state.view.drop(1)).takeWhile { (prev, next) => prev.ix + 1 == next.ix }
        consecutive.lastOption match
          case Some((_, value)) =>
            val advancing = state.view.slice(1, consecutive.size + 1)
            val seenAts   = advancing.map(_.seenAts).fold(Seq.empty)(_ ++ _)
            val txs       = advancing.map(_.txSpans).fold(Seq.empty)(_ ++ _)
            val batches   = advancing.map(_.persistSpans).fold(Seq.empty)(_ ++ _).distinct
            state.remove(0, consecutive.size)
            val effectiveWatermark = value.copy(seenAts = seenAts, txSpans = txs, persistSpans = batches)
            ZChannel.write(Chunk(effectiveWatermark)) *> accumulator(state)
          case None =>
            accumulator(state)
      },
      err => ZChannel.refailCause(err),
      _ => ZChannel.unit
    )
    ZPipeline.unwrap(
      getLastCheckpoint
        .map(cp => model.Watermark(cp._2, cp._1, Seq.empty))
        .map(start =>
          ZPipeline.fromChannel[Any, Nothing, Chunk[model.Watermark], model.Watermark](
            accumulator(mutable.ArrayBuffer(start))
          )
        )
    )

  private def handleWatermarks =
    val trackWatermark = latency("pipeline_progress_watermark", "Latency of watermark progression")
    val watermarkIx = Metric
      .gauge("watermark_ix", "Current watermark index (transaction ordinal number for consistent reads)")
      .contramap[Long](_.toDouble)
    val txProcessingLatency = Metric
      .histogram(
        "total_tx_handling_latency",
        "Total transaction handling latency in pqs",
        Boundaries.exponential(0.001, math.pow(10, 1.0 / 3), 13)
      )
      .contramap[Long](_.toDouble / 1e9)
    ZPipeline[model.Watermark].mapZIO(wm =>
      traces.span("advance datastore watermark") {
        trackWatermark(updateWatermark(wm))
          @@ traces.attributes(
            "pqs.watermark.offset" -> wm.offset.toSqlValue,
            "pqs.watermark.ix"     -> wm.ix
          )
          *> ZIO.foreachDiscard(wm.txSpans) { s =>
            s.linkToCurrentSpan("target" -> "↧ advance watermark")
              *> s.addEvent(
                "advanced datastore watermark",
                "offset" -> wm.offset.toSqlValue,
                "index"  -> wm.ix
              )
              *> s.end()
          }
          *> ZIO.foreachDiscard(wm.persistSpans) { s =>
            ZIO.unit @@ traces.link(s, "target" -> "↥ persist to datastore")
          }
          *> zio.Clock.nanoTime.flatMap(now =>
            ZIO.foreachDiscard(wm.seenAts) { seenAt => txProcessingLatency.update(now - seenAt) }
          )
          *> watermarkIx.update(wm.ix)
          *> logInfo(s"Advanced watermark: ix = ${wm.ix}, offset = ${wm.offset.toSqlValue}")
      }
    )

  private def updateWatermark(wm: model.Watermark) =
    tx(sql"update __rel_watermark set ledger_offset = ${wm.offset.toSqlValue}, tx_ix = ${wm.ix}".update)
      .filterOrFail(_ == 1)(RuntimeException("Failed to update watermark."))

  private def updateAcsOffsets =
    ZPipeline[model.Watermark].tap(wm =>
      tx(sql"update __rel_transactions set ledger_offset = ${wm.offset.toSqlValue} where tx_ix = ${Genesis._2}".update)
    )

  private def convertTransactionToSqlStatements(
      tx: canonical.specific.Transaction[canonical.specific.Event | TreeEvent | ReassignmentEvent],
      txIx: Long
  ): Chunk[model.Model] =
    val insertTx = model.Transaction(
      specific.Transaction(
        txIx,
        tx.offset,
        Some(tx.transactionId),
        tx.effectiveAt,
        Some(tx.workflowId),
        tx.externalTransactionHash,
        tx.paidTrafficCost,
        tx.domainId
      ),
      Some(tx.span)
    )
    val insertEvents    = tx.events.flatMap(evt => insertEvent(txIx, model.SourceKind.Stream, tx.domainId, evt))
    val insertWatermark = model.Watermark(txIx, tx.offset, Seq(tx.seenAt))
    insertTx +: insertEvents :+ insertWatermark

  private def insertEvent(
      txIx: Long,
      sourceKind: model.SourceKind,
      synchronizerId: Option[String],
      event: canonical.specific.Event | TreeEvent | ReassignmentEvent
  ): Chunk[model.Model] = {
    val eventPk = placeholders.mk

    def eventRow(
        eid: canonical.specific.EventId,
        contractId: ContractId,
        templateEntityPk: specific.EntityTypePk,
        eventKind: model.EventKind,
        archiveSource: Option[model.ArchiveSource]
    ) =
      model.Event(
        specific.Event(
          pk = eventPk,
          txIx = txIx,
          ledgerOffset = eid._1,
          nodeId = eid._2,
          contractId = contractId,
          templateEntityPk = templateEntityPk,
          eventKind = eventKind,
          sourceKind = sourceKind,
          archiveSource = archiveSource,
          visibilityComplete = true
        )
      )

    def eventVisibility(witnesses: Chunk[Party]) =
      witnesses.map(p => model.EventVisibility(specific.EventVisibility(eventPk, p)))

    event match
      case c: canonical.specific.Event.Created =>
        c.payloads.find { (id, _) => isTemplate(id) }.fold(Chunk.empty[model.Model]) { (templateId, templateDv) =>
          val templateEntityPk = getEntityPk(templateId)
          // the contract reuses its create event's pk, so a create allocates one id, not two
          val contractPk = eventPk
          val historyLowerBound = sourceKind match
            case model.SourceKind.AcsSeed => true
            case _                        => false
          val contract = model.Contract(
            specific.Contract(
              contractPk = contractPk,
              contractId = c.contractId,
              templateEntityPk = templateEntityPk,
              representativePackageId = c.representativePackageId,
              creationPackageId = c.creationPackageId,
              createdAtIx = txIx,
              createdAtOffset = sourceKind match
                case model.SourceKind.AcsSeed => None
                case _                        => Some(c.eventId._1),
              signatories = c.signatories,
              observers = c.observers,
              createWitnesses = c.witnesses,
              metadata = c.metadata,
              contractKey = (codec.getTemplateKey(templateId) zip c.contractKey).map(_ `fromDynamicValue` _),
              contractKeyHash = codec.getTemplateKey(templateId).flatMap(_ => c.contractKeyHash),
              acsDelta = c.acsDelta,
              sourceKind = sourceKind,
              synchronizerId = synchronizerId,
              historyLowerBound = historyLowerBound
            )
          )
          val contractVisibility =
            c.signatories.map(p =>
              model.ContractVisibility(specific.ContractVisibility(contractPk, p, model.VisibilityRole.Signatory))
            ) ++
              c.observers.map(p =>
                model.ContractVisibility(specific.ContractVisibility(contractPk, p, model.VisibilityRole.Observer))
              ) ++
              c.witnesses.map(p =>
                model.ContractVisibility(specific.ContractVisibility(contractPk, p, model.VisibilityRole.Witness))
              )
          val payload = model.ContractPayload(
            specific.ContractPayload(
              contractPk,
              codec.template(templateId).fromDynamicValue(templateDv),
              promotedFor(templateId, templateDv)
            ),
            getBaseTable(templateId)
          )
          val views = c.payloads.filter { (id, _) => !isTemplate(id) }.map { (interfaceId, viewDv) =>
            model.InterfaceView(
              specific.InterfaceView(contractPk, codec.template(interfaceId).fromDynamicValue(viewDv)),
              getViewTable(interfaceId)
            )
          }
          Chunk(
            eventRow(c.eventId, c.contractId, templateEntityPk, model.EventKind.Create, None),
            contract,
            payload
          ) ++ views ++ eventVisibility(c.witnesses) ++ contractVisibility
        }

      case a: canonical.specific.Event.Archived =>
        Chunk(
          eventRow(
            a.eventId,
            a.contractId,
            getEntityPk(a.templateId),
            model.EventKind.Archive,
            Some(model.ArchiveSource.Native)
          ),
          model.TmpLifecycle(specific.TmpLifecycle(a.contractId, txIx, Some(a.eventId._1)))
        ) ++ eventVisibility(a.witnesses)

      case e: canonical.specific.Event.Exercised =>
        val exercise = model.Exercise(
          specific.Exercise(
            exerciseEventPk = eventPk,
            choiceEntityPk = getExercisePk(e.entityId, e.choice),
            contractTemplateEntityPk = getEntityPk(e.templateId),
            choiceName = e.choice,
            consuming = e.consuming,
            controllers = e.controllers,
            argument = codec.choiceArgument(e.entityId, e.choice).fromDynamicValue(e.arg),
            result = codec.choiceResult(e.entityId, e.choice).fromDynamicValue(e.result),
            lastDescendant = e.lastDescendant
          )
        )
        val archiveSource = Option.when(e.consuming)(model.ArchiveSource.ConsumingExercise)
        val lifecycle =
          if e.consuming then Chunk(model.TmpLifecycle(specific.TmpLifecycle(e.contractId, txIx, Some(e.eventId._1))))
          else Chunk.empty
        Chunk(
          eventRow(e.eventId, e.contractId, getEntityPk(e.templateId), model.EventKind.Exercise, archiveSource),
          exercise
        ) ++ eventVisibility(e.witnesses) ++ lifecycle

      case _: ReassignmentEvent =>
        Chunk.empty
  }
end RelationalPostgres

object RelationalPostgres:
  val live = ZLayer.scoped {
    traces.root("process metadata and schema") {
      for
        config     <- ZIO.service[SchemaConfig]
        poolConfig <- ZIO.service[PostgresConfig]
        instanceId <- ZIO.service[InstanceId]
        pool       <- ZIO.service[ZConnectionPool]
        schema     <- ZIO.service[RelSqlSchema]
        codec      <- ZIO.service[Dictionary[Codec[Value]]]

        fenceEnv <- WriterFence.acquire(pool, poolConfig.maxConnections)

        _ <- RelationalSchema.applySchema(poolConfig, instanceId, config.baseline) when config.autoApply

        entities <- transaction {
          sql"""select pkg.id, e.module_name, e.entity_name, e.pk, e.base_table
                from __rel_entity e join __rel_package pkg on e.package_name = pkg.name
                where e.kind = 'template'"""
            .query[(String, String, String, specific.EntityTypePk, String)]
            .selectAll
        }
        entityMap    = entities.map((pkg, module, entity, pk, _) => (pkg, module, entity) -> pk).toMap
        baseTableMap = entities.map((pkg, module, entity, _, tbl) => (pkg, module, entity) -> tbl).toMap
        getEntityPk  = (id: Identifier) => entityMap((id.packageId, id.moduleName, id.entityName))
        getBaseTable = (id: Identifier) => baseTableMap((id.packageId, id.moduleName, id.entityName))
        isTemplate   = (id: Identifier) => entityMap.contains((id.packageId, id.moduleName, id.entityName))
        _ <- logInfo(s"Initialised ${entities.size} template entity types")

        interfaces <- transaction {
          sql"""select pkg.id, e.module_name, e.entity_name, e.base_table
                from __rel_entity e join __rel_package pkg on e.package_name = pkg.name
                where e.kind = 'interface'"""
            .query[(String, String, String, String)]
            .selectAll
        }
        viewTableMap = interfaces.map((pkg, module, entity, tbl) => (pkg, module, entity) -> tbl).toMap
        getViewTable = (id: Identifier) => viewTableMap((id.packageId, id.moduleName, id.entityName))
        _ <- logInfo(s"Initialised ${interfaces.size} interface entity types")

        exercises <- transaction {
          sql"""select pkg.id, e.module_name, e.entity_name, c.choice, c.pk
                from __rel_choice c
                join __rel_entity e on e.pk = c.entity_pk
                join __rel_package pkg on pkg.name = e.package_name"""
            .query[(String, String, String, String, specific.EntityTypePk)]
            .selectAll
        }
        exerciseMap = exercises.map((pkg, module, entity, choice, pk) => (pkg, module, entity, choice) -> pk).toMap
        getExercisePk = (id: Identifier, choice: ChoiceName) =>
          exerciseMap((id.packageId, id.moduleName, id.entityName, choice))
        _ <- logInfo(s"Initialised ${exercises.size} exercise types")

        // events and contracts share one id space, so seed the allocator past the max of both tables
        lastId <- transaction {
          sql"""select greatest(
                  coalesce((select max(event_pk) from __query_events), 0),
                  coalesce((select max(contract_pk) from __rel_contracts), 0))""".query[Long].selectOne.someOrElse(0L)
        }
        placeholders = IdPlaceholder.factory(lastId)

        projectionShapes <- ProjectionBinding.activeShapes.provideEnvironment(fenceEnv)
        _                <- fenceEnv.get[ZConnection].access(_.commit())
        _                <- logInfo(s"Bound ${projectionShapes.size} active projection shape(s)")
      yield RelationalPostgres(
        config,
        poolConfig,
        pool,
        schema,
        codec,
        getEntityPk,
        getExercisePk,
        getBaseTable,
        getViewTable,
        isTemplate,
        placeholders,
        projectionShapes
      )
    }
  }

end RelationalPostgres
