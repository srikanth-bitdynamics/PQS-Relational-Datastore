// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.relational

import com.digitalasset.pqs.o11y.traces
import com.digitalasset.pqs.postgres.backend.*
import io.github.classgraph.ClassGraph
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.ResourceProvider
import org.flywaydb.core.api.resource.LoadableResource
import org.flywaydb.core.internal.jdbc.DriverDataSource
import zio.ZIO.logInfo
import zio.{Task, ZIO}

import java.io.{Reader, StringReader}
import java.util
import scala.jdk.CollectionConverters.*
import scala.util.Using

object RelationalSchema:
  private val migrationPath = "db/relational"

  def applySchema(pgCfg: PostgresConfig, instanceId: InstanceId, doBaseline: Boolean): Task[Unit] =
    traces.span("apply relational schema") {
      logInfo("Applying relational schema") *>
        ZIO.attemptBlocking {
          Flyway
            .configure()
            .dataSource(
              DriverDataSource(
                Thread.currentThread().getContextClassLoader,
                "org.postgresql.Driver",
                s"jdbc:postgresql://${pgCfg.host}:${pgCfg.port}/${pgCfg.database}?currentSchema=${pgCfg.schema}",
                pgCfg.username,
                pgCfg.password.value,
                (sslprops(pgCfg.tls) ++ instanceIdProp(instanceId)).asJava
              )
            )
            .baselineOnMigrate(doBaseline)
            .baselineVersion("001")
            .baselineDescription("Baseline relational schema")
            .resourceProvider(new ResourceProvider {
              @SuppressWarnings(Array("org.wartremover.warts.Null"))
              def getResource(name: String): LoadableResource = null
              @SuppressWarnings(Array("org.wartremover.warts.TryPartial"))
              def getResources(prefix: String, suffixes: Array[String]): util.Collection[LoadableResource] =
                Using
                  .Manager { use =>
                    val scanResult = use(ClassGraph().acceptPaths(migrationPath).scan())
                    Seq(suffixes*)
                      .flatMap(suffix => scanResult.getResourcesWithExtension(suffix).asScala.toSeq)
                      .sortBy(_.getPath)
                      .map(x =>
                        new LoadableResource {
                          private val contents              = use(x).getContentAsString
                          def read(): Reader                = StringReader(contents)
                          def getAbsolutePath: String       = x.getURL.toString
                          def getAbsolutePathOnDisk: String = x.getClasspathElementFile.getAbsolutePath
                          def getFilename: String           = x.getPath.split('/').last
                          def getRelativePath: String       = x.getPath.drop(migrationPath.length + 1)
                        }
                      )
                  }
                  .get
                  .asJava
            })
            .load()
            .migrate()
        }.unit
    } <* logInfo("Relational schema applied")
end RelationalSchema
