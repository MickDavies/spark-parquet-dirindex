package org.apache.spark.sql.parquet.dirindex

import org.apache.spark.sql._
import org.apache.spark.sql.parquet.DirIndexParquetRelation
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider}

/**
 * Allows creation of parquet based tables using the syntax
 * `CREATE TEMPORARY TABLE ... USING org.apache.spark.sql.parquet.dirindex`. The options specified must
 * be
 * <li> path - the path under which all files reside
 * <li> col1 - The name:type of the column represented in the sub-directory under path
 * <li> col2 - The name:type of the column represented in the sub-sub-directory under path
 * <li> col3 ...
 *
 * The class is based closely on [[org.apache.spark.sql.parquet.DefaultSource]]
 *
 */

object DefaultSource {
  val colPrefix = "col"
  val nameTypeRE = "([^:]+):([^:]+)".r

  def dataType(dataTypeName: String): DataType = DataType.fromCaseClassString(dataTypeName + "Type")

  def extractColumns(parameters: Map[String, String], col: Int): List[(String, DataType)] = parameters.get("col" + col) match {
    case None => Nil
    case Some(nameTypeRE(colName, dataTypeName)) => (colName, dataType(dataTypeName)) :: extractColumns(parameters, col + 1)
    case Some(s) => throw new IllegalArgumentException(s"Unable to extract column from 'col$col' with value '$s'")
  }
}

class DefaultSource extends RelationProvider {

  import org.apache.spark.sql.parquet.dirindex.DefaultSource._

  /** Returns a new base relation with the given parameters. */
  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {
    val path = parameters.getOrElse("path", sys.error("'path' must be specified for directory indexed parquet tables."))
    val directoryColumns = extractColumns(parameters, 1)
    DirIndexParquetRelation(path, directoryColumns)(sqlContext)
  }
}
