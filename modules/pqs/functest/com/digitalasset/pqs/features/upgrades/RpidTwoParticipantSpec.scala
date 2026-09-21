// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features.upgrades

import com.digitalasset.pqs.functest.FuncTestStandalone
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.*
import com.digitalasset.pqs.services.pqs.{Pipeline, Pqs}
import zio.*
import zio.jdbc.sqlInterpolator
import zio.test.*

import scala.language.implicitConversions

/** This test must remain standalone because it starts a ledger with 2 participants and a custom bootstrap script. The
  * two-participant fixture is shared with the relational backend via [[TwoParticipantRpid]].
  */
object RpidTwoParticipantSpec extends FuncTestStandalone:

  override protected def layerTimeout = 10.minutes

  def spec = suite("RpidTwoParticipantSpec")(
    funcTest("creation_package_id differs from representative_package_id in two-participant setup"):
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
        Pqs.pipeline(
          "--pipeline-datasource=TransactionStream",
          "--pipeline-filter-parties=*",
          s"--pipeline-filter-contracts=${Pipeline.allContractsWithoutAdminWorkflows}"
        )
      And:
        Pqs `hasProcessedAtLeastTransactions` 1
      And:
        DamlSdk.runScript("RpidTest:create", alice.id)
      And:
        Pqs `hasProcessedAtLeastTransactions` 2
      Expect:
        apiCreates() `returns` table {
          "package_id"        | "creation_package_id"
          ---                 | ---
          v2Dar.get.packageId | v1Dar.get.packageId
          v2Dar.get.packageId | v2Dar.get.packageId
        }
      And:
        apiActive() `returns` table {
          "package_id"        | "creation_package_id"
          ---                 | ---
          v2Dar.get.packageId | v1Dar.get.packageId
          v2Dar.get.packageId | v2Dar.get.packageId
        }
      And:
        storageContracts() `returns` table {
          "representative_package_id" | "creation_package_id" | "status"
          ---                         | ---                   | ---
          v2Dar.get.packageId         | v1Dar.get.packageId   | "active"
          v2Dar.get.packageId         | null                  | "active"
        }
      When:
        DamlSdk.runScript("RpidTest:consumeAll", alice.id)
      And:
        Pqs `hasProcessedAtLeastTransactions` 4
      Expect:
        apiArchives() `returns` table {
          "package_id"        | "creation_package_id"
          ---                 | ---
          v2Dar.get.packageId | v1Dar.get.packageId
          v2Dar.get.packageId | v2Dar.get.packageId
        }
      And:
        apiActive() `returns` Table.empty
      And:
        storageContracts() `returns` table {
          "representative_package_id" | "creation_package_id" | "status"
          ---                         | ---                   | ---
          v2Dar.get.packageId         | v1Dar.get.packageId   | "archived"
          v2Dar.get.packageId         | null                  | "archived"
        }
  )

  private def storageContracts() = Postgres `query`
    sql"""SELECT
            p.id as representative_package_id,
            c.creation_package_id,
            CASE WHEN c.archived_at_ix IS NULL THEN 'active' ELSE 'archived' END as status
          FROM __contracts c
          JOIN __packages p ON c.package_pk = p.pk
          ORDER BY created_at_ix"""

  private def apiActive() = Postgres `query`
    sql"""SELECT
            package_id,
            creation_package_id
          FROM active()
          ORDER BY created_at_ix"""

  private def apiCreates() = Postgres `query`
    sql"""SELECT
            package_id,
            creation_package_id
          FROM creates()
          ORDER BY created_at_ix"""

  private def apiArchives() = Postgres `query`
    sql"""SELECT
            package_id,
            creation_package_id
          FROM archives()
          ORDER BY created_at_ix"""

end RpidTwoParticipantSpec
