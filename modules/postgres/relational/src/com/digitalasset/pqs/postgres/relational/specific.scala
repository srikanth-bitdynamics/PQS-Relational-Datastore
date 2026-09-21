package com.digitalasset.pqs.postgres.relational

import com.digitalasset.canonical.specific.{NodeId, Offset}
import com.digitalasset.canonical.{ContractId, Party}
import com.digitalasset.pqs.postgres.backend.IdPlaceholder
import com.digitalasset.pqs.postgres.relational.model
import com.digitalasset.pqs.postgres.relational.model.{toSqlValue, given}
import com.digitalasset.transcode.schema.ChoiceName
import ujson.Value

import java.time.Instant

object specific:
  type EntityTypePk = Long

  final class Transaction(
      val ix: Long,
      val offset: Offset,
      transactionId: Option[String],
      effectiveAt: Option[Instant],
      workflowId: Option[String],
      externalTransactionHash: Option[Array[Byte]],
      paidTrafficCost: Option[Long],
      synchronizerId: Option[String]
  ):
    val columns = Seq(
      "tx_ix",
      "ledger_offset",
      "transaction_id",
      "effective_at",
      "synchronizer_id",
      "workflow_id",
      "external_transaction_hash",
      "paid_traffic_cost"
    )
    val rowValues = model.values(ix)(offset.toSqlValue)(transactionId)(effectiveAt)(synchronizerId)(workflowId)(
      externalTransactionHash
    )(paidTrafficCost)

  final case class Event(
      pk: IdPlaceholder,
      txIx: Long,
      ledgerOffset: Long,
      nodeId: NodeId,
      contractId: ContractId,
      templateEntityPk: EntityTypePk,
      eventKind: model.EventKind,
      sourceKind: model.SourceKind,
      archiveSource: Option[model.ArchiveSource],
      visibilityComplete: Boolean
  ):
    val columns = Seq(
      "event_pk",
      "tx_ix",
      "ledger_offset",
      "node_id",
      "contract_id",
      "template_entity_pk",
      "event_kind",
      "source_kind",
      "archive_source",
      "visibility_complete"
    )
    val rowValues = model.values(pk)(txIx)(ledgerOffset)(nodeId)(contractId)(templateEntityPk)(eventKind)(sourceKind)(
      archiveSource
    )(visibilityComplete)

  final case class EventVisibility(eventPk: IdPlaceholder, party: Party):
    val columns   = Seq("event_pk", "party")
    val rowValues = model.values(eventPk)(party)

  final case class Contract(
      contractPk: IdPlaceholder,
      contractId: ContractId,
      templateEntityPk: EntityTypePk,
      representativePackageId: String,
      creationPackageId: Option[String],
      createdAtIx: Long,
      createdAtOffset: Option[Long],
      signatories: Seq[Party],
      observers: Seq[Party],
      createWitnesses: Seq[Party],
      metadata: Option[Array[Byte]],
      contractKey: Option[Value],
      contractKeyHash: Option[Array[Byte]],
      acsDelta: Boolean,
      sourceKind: model.SourceKind,
      synchronizerId: Option[String],
      historyLowerBound: Boolean
  ):
    val columns = Seq(
      "contract_pk",
      "contract_id",
      "template_entity_pk",
      "representative_package_id",
      "creation_package_id",
      "created_tx_ix",
      "created_at_offset",
      "signatories",
      "observers",
      "create_witnesses",
      "creation_synchronizer_id",
      "metadata",
      "contract_key_json",
      "contract_key_hash",
      "divulged_only",
      "source_kind",
      "history_lower_bound"
    )
    val rowValues =
      model.values(contractPk)(contractId)(templateEntityPk)(representativePackageId)(creationPackageId)(createdAtIx)(
        createdAtOffset
      )(signatories)(observers)(createWitnesses)(synchronizerId)(metadata)(contractKey)(contractKeyHash)(
        !acsDelta
      )(sourceKind)(historyLowerBound)

  final case class ContractVisibility(contractPk: IdPlaceholder, party: Party, role: model.VisibilityRole):
    val columns   = Seq("contract_pk", "party", "role")
    val rowValues = model.values(contractPk)(party)(role)

  final case class Exercise(
      exerciseEventPk: IdPlaceholder,
      choiceEntityPk: EntityTypePk,
      contractTemplateEntityPk: EntityTypePk,
      choiceName: ChoiceName,
      consuming: Boolean,
      controllers: Seq[Party],
      argument: Value,
      result: Value,
      lastDescendant: NodeId
  ):
    val columns = Seq(
      "event_pk",
      "choice_entity_pk",
      "contract_template_entity_pk",
      "choice_name",
      "consuming",
      "controllers",
      "argument_json",
      "result_json",
      "last_descendant_node_id"
    )
    val rowValues = model.values(exerciseEventPk)(choiceEntityPk)(contractTemplateEntityPk)(choiceName)(consuming)(
      controllers
    )(argument)(result)(lastDescendant)

  final case class TmpLifecycle(contractId: ContractId, archivedTxIx: Long, archivedAtOffset: Option[Long]):
    val columns   = Seq("contract_id", "archived_tx_ix", "archived_at_offset")
    val rowValues = model.values(contractId)(archivedTxIx)(archivedAtOffset)

  final case class ContractPayload(contractPk: IdPlaceholder, payloadJson: Value):
    val columns   = Seq("contract_pk", "payload_json")
    val rowValues = model.values(contractPk)(payloadJson)

  final case class InterfaceView(contractPk: IdPlaceholder, viewJson: Value):
    val columns   = Seq("contract_pk", "view_json")
    val rowValues = model.values(contractPk)(viewJson)
end specific
