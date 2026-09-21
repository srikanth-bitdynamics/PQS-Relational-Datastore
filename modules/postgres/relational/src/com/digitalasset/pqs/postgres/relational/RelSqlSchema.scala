package com.digitalasset.pqs.postgres.relational

import com.digitalasset.transcode.schema.*
import io.github.classgraph.{ClassGraph, Resource}

import java.nio.charset.StandardCharsets
import scala.util.Using

final case class RelSqlSchema(schema: String, mappings: String)

object RelSqlSchema extends SchemaVisitor.Unit:
  type Result = RelSqlSchema

  def collect(entities: Seq[Template[Unit]]) =
    val packages = entities
      .flatMap(t => t.templateId +: t.implements)
      .distinctBy(_.packageId)
      .map(id => (id.packageId, id.packageName, id.packageVersion))
    val getPackageName = packages.map((id, name, version) => id -> name).toMap.apply
    RelSqlSchema(
      schema = migrations,
      mappings = (packages.map(initPackage) ++ entities.flatMap(initEntity(getPackageName))).mkString(
        s"-- DAML<=>PG mappings${System.lineSeparator}do $$$$ begin${System.lineSeparator}",
        System.lineSeparator,
        s"${System.lineSeparator}end; $$$$"
      )
    )

  private[relational] def lit(v: Any): String = "'" + v.toString.replace("'", "''") + "'"

  private def initPackage(id: PackageId, name: PackageName, version: PackageVersion) =
    s"call __rel_initialize_package(${lit(name)}, ${lit(version)}, ${lit(id)});"

  private def initEntity(getPackageName: PackageId => PackageName)(entity: Template[Unit]) =
    Seq(
      s"""call __rel_initialize_entity(
         |  ${lit(getPackageName(entity.templateId.packageId))},
         |  ${lit(entity.templateId.moduleName)},
         |  ${lit(entity.templateId.entityName)},
         |  ${lit(if entity.isInterface then "interface" else "template")}
         |);""".stripMargin
    ) ++ entity.implements.map { interfaceId =>
      s"""call __rel_initialize_entity(
         |  ${lit(getPackageName(interfaceId.packageId))},
         |  ${lit(interfaceId.moduleName)},
         |  ${lit(interfaceId.entityName)},
         |  ${lit("interface")}
         |);""".stripMargin
    } ++ entity.implements.map { interfaceId =>
      s"""call __rel_initialize_implements(
         |  ${lit(getPackageName(entity.templateId.packageId))},
         |  ${lit(entity.templateId.moduleName)},
         |  ${lit(entity.templateId.entityName)},
         |  ${lit(getPackageName(interfaceId.packageId))},
         |  ${lit(interfaceId.moduleName)},
         |  ${lit(interfaceId.entityName)}
         |);""".stripMargin
    } ++ entity.choices.map { c =>
      s"""call __rel_initialize_choice(
         |  ${lit(getPackageName(entity.templateId.packageId))},
         |  ${lit(entity.templateId.moduleName)},
         |  ${lit(entity.templateId.entityName)},
         |  ${lit(if entity.isInterface then "interface" else "template")},
         |  ${lit(c.name)},
         |  ${c.consuming}
         |);""".stripMargin
    }

  private lazy val migrations =
    val sb  = StringBuilder()
    val sep = System.lineSeparator
    Using.Manager { use =>
      val scanResult = use(ClassGraph().acceptPaths("db/relational").scan())
      val migrations = scanResult.getResourcesWithExtension("sql")
      migrations.sort((a, b) => a.getPath.compareTo(b.getPath))
      migrations.forEachByteArrayThrowingIOException((res: Resource, content: Array[Byte]) => {
        val path       = res.getPath
        val frameStart = "-- " + ">".repeat(path.length + 8) + " --"
        val frameEnd   = "-- " + "<".repeat(path.length + 6) + " --"
        sb ++= s"$sep$frameStart$sep"
        sb ++= s"-- $path (start) --"
        sb ++= s"$sep$frameStart$sep$sep"
        sb ++= String(content, StandardCharsets.UTF_8)
        sb ++= sep
        sb ++= s"$sep$frameEnd$sep"
        sb ++= s"-- $path (end) --"
        sb ++= s"$sep$frameEnd$sep$sep"
      })
    }
    sb.result()

end RelSqlSchema
