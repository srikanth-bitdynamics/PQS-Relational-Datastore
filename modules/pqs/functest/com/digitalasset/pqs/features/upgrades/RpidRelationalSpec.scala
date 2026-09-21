// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features.upgrades

import com.digitalasset.pqs.functest.{FuncTest, FuncTestStandalone}
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.daml.DamlSdk.onlyCantonVersion
import com.digitalasset.pqs.services.postgres.*
import com.digitalasset.pqs.services.pqs.{Pipeline, Pqs}
import zio.*
import zio.jdbc.sqlInterpolator
import zio.test.*

import scala.language.{implicitConversions, postfixOps}

/** Relational counterpart of [[RpidTwoParticipantSpec]]: the same two-participant offline-party-replication fixture,
  * asserting that the relational store preserves a representative package (v2) distinct from the creation package (v1).
  */
object RpidRelationalSpec extends FuncTestStandalone:

  override protected def layerTimeout = 10.minutes

  def spec = suite("relational rpid spec")(
    funcTest("preserves distinct representative and creation package ids across an upgrade"):
      val alice      = Party("Alice")
      lazy val v1Dar = Capture[DarFile]
      lazy val v2Dar = Capture[DarFile]
      Given:
        Postgres.instance
      And:
        DamlSdk.dar(TwoParticipantRpid.packageV1)
      And:
        v1Dar.captureFromService
      And:
        DamlSdk.dar(TwoParticipantRpid.packageV2)
      And:
        v2Dar.captureFromService
      And:
        TwoParticipantRpid.cantonParticipantWithACSImportContract(alice, v1Dar.get, v2Dar.get) ++ Postgres.database
      When:
        Pqs.attemptRelationalPipeline(
          "--pipeline-datasource=TransactionStream",
          "--pipeline-filter-parties=*",
          s"--pipeline-filter-contracts=${Pipeline.allContractsWithoutAdminWorkflows}"
        )
      And:
        DamlSdk.runScript("RpidTest:create", alice.id)
      Expect:
        // the imported contract keeps v1 as its creation package under the v2 representative; the native v2 create
        // stores no creation package (it equals the representative)
        FuncTest.retryUntilTimeout(
          storageContracts() `returns` table {
            "representative_package_id" | "creation_package_id" | "status"
            ---                         | ---                   | ---
            v2Dar.get.packageId         | v1Dar.get.packageId   | "active"
            v2Dar.get.packageId         | null                  | "active"
          }
        )
      When:
        DamlSdk.runScript("RpidTest:consumeAll", alice.id)
      Expect:
        FuncTest.retryUntilTimeout(
          storageContracts() `returns` table {
            "representative_package_id" | "creation_package_id" | "status"
            ---                         | ---                   | ---
            v2Dar.get.packageId         | v1Dar.get.packageId   | "archived"
            v2Dar.get.packageId         | null                  | "archived"
          }
        )
  ) @@ onlyCantonVersion(">=3.5")

  private def storageContracts() = Postgres `query`
    sql"""SELECT
            representative_package_id,
            creation_package_id,
            CASE WHEN archived_tx_ix IS NULL THEN 'active' ELSE 'archived' END as status
          FROM pqs_relational.__rel_contracts
          ORDER BY created_tx_ix"""

end RpidRelationalSpec
