package org.apache.spark.sql.parquet

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec, Matchers}


/** End to end tests for DirIndexParquet */
class DirIndexParquetTest extends FlatSpec with Matchers with BeforeAndAfter with BeforeAndAfterAll with HDFSTester {
  var sqlContext: SQLContext = null

  "A DirIndexParquet" should "handle the definition of a single column with a single dir" in {
    val path = mkDir("c1v1")
    createParquetFile(sqlContext, path, Row("c1v1", "rv1"), Row("c1v1", "rv2"))

    sqlContext.sql( s"""
      create temporary table dirindex_parquet
      USING org.apache.spark.sql.parquet.dirindex
      OPTIONS (
        path '${basePath.toString}',
        col1 'c1:String'
      )
    """)
    val results = sqlContext.sql("SELECT * FROM dirindex_parquet WHERE c2 = 'rv2'")
    results.collect should be(List(Row("c1v1", "rv2")))

  }

  it should "handle the definition of a single column with with multiple values" in {
    val c1v1 = mkDir("c1v1")
    createParquetFile(sqlContext, c1v1, Row("c1v1", "rv1"), Row("c1v1", "rv2"))
    val c1v2 = mkDir("c1v2")
    createParquetFile(sqlContext, c1v2, Row("c1v2", "rv1"), Row("c1v2", "rv2"))

    sqlContext.sql( s"""
      create temporary table dirindex_parquet
      USING org.apache.spark.sql.parquet.dirindex
      OPTIONS (
        path '${basePath.toString}',
        col1 'c1:String'
      )
    """)
    val results = sqlContext.sql("SELECT * FROM dirindex_parquet WHERE c2 = 'rv2'")
    results.collect should be(List(Row("c1v1", "rv2"), Row("c1v2", "rv2")))
  }

  it should "support queries with dir column restrictions for a single dir column" in {
    val c1v1 = mkDir("c1v1")
    createParquetFile(sqlContext, c1v1, Row("c1v1", "rv1"), Row("c1v1", "rv2"))
    val c1v2 = mkDir("c1v2")
    createParquetFile(sqlContext, c1v2, Row("c1v2", "rv1"), Row("c1v2", "rv2"))

    sqlContext.sql( s"""
      create temporary table dirindex_parquet
      USING org.apache.spark.sql.parquet.dirindex
      OPTIONS (
        path '${basePath.toString}',
        col1 'c1:String'
      )
    """)
    val results = sqlContext.sql("SELECT * FROM dirindex_parquet WHERE c1 = 'c1v1'")
    results.collect should be(List(Row("c1v1", "rv1"), Row("c1v1", "rv2")))
  }

  it should "handle the definition of a 2 columns with multiple values" in {
    val v1 = mkDir("c1v1/c2v1")
    createParquetFile(sqlContext, v1, Row("c1v1", "c2v1", "rv1"), Row("c1v1", "c2v1", "rv2"))
    val v2 = mkDir("c1v1/c2v2")
    createParquetFile(sqlContext, v2, Row("c1v1", "c2v2", "rv1"), Row("c1v1", "c2v2", "rv2"))
    val v3 = mkDir("c1v2/c2v3")
    createParquetFile(sqlContext, v3, Row("c1v2", "c2v3", "rv1"), Row("c1v2", "c2v3", "rv2"))

    sqlContext.sql( s"""
      create temporary table dirindex_parquet
      USING org.apache.spark.sql.parquet.dirindex
      OPTIONS (
        path '${basePath.toString}',
        col1 'c1:String',
        col2 'c2:String'
     )
    """)
    sqlContext.sql("SELECT * FROM dirindex_parquet WHERE c1 = 'c1v1'").count should be(4)
    sqlContext.sql("SELECT * FROM dirindex_parquet WHERE c1 = 'c1v2'").count should be(2)
    sqlContext.sql("SELECT * FROM dirindex_parquet WHERE c2 = 'c2v1'").count should be(2)
  }

  it should "handle the definition of a 2 columns of different types with multiple values" in {
    val v1 = mkDir("c1v1/1")
    createParquetFile(sqlContext, v1, Row("c1v1", 1, "rv1"), Row("c1v1", 1, "rv2"))
    val v2 = mkDir("c1v1/2")
    createParquetFile(sqlContext, v2, Row("c1v1", 2, "rv1"), Row("c1v1", 2, "rv2"))
    val v3 = mkDir("c1v2/3")
    createParquetFile(sqlContext, v3, Row("c1v2", 3, "rv1"), Row("c1v2", 3, "rv2"))

    sqlContext.sql( s"""
      create temporary table dirindex_parquet
      USING org.apache.spark.sql.parquet.dirindex
      OPTIONS (
        path '${basePath.toString}',
        col1 'c1:String',
        col2 'c2:Integer'
     )
    """)

    sqlContext.sql("SELECT * FROM dirindex_parquet WHERE c2 = 2").count should be(2)
  }

  it should "raise an error if a dir name cannot be converted to expected type" in {
    val v1 = mkDir("c1v1/invalidDirNameForInt")
    createParquetFile(sqlContext, v1, Row("c1v1", 1, "rv1"), Row("c1v1", 1, "rv2"))

    intercept[java.lang.IllegalArgumentException] {
      sqlContext.sql( s"""
      create temporary table dirindex_parquet
      USING org.apache.spark.sql.parquet.dirindex
      OPTIONS (
        path '${basePath.toString}',
        col1 'c1:String',
        col2 'c2:Integer'
     )
    """)
    }.getMessage should include("dir name invalidDirNameForInt cannot be converted to type IntegerType")


  }

  // TODO test when dir name cannot be converted to type
  // TODO test large number of dirs
  // How do we handle nulls -
  // Can we escape strings in file names


  def createParquetFile(sqlContext: SQLContext, path: Path, rows: Row*) {
    val rowRDD = sqlContext.sparkContext.parallelize(rows)

    val typeInfo = for (col <- 1 to rows.head.size) yield ("c" + col, rows.head(col - 1) match {
      case _: String => StringType
      case _: Integer => IntegerType
    })

    val schema =
      StructType(
        typeInfo.map(ti => StructField(ti._1, ti._2, nullable = true)))
    val schemaRDD = sqlContext.applySchema(rowRDD, schema)
    val filePath = new Path(path, "test1.parquet")
    schemaRDD.saveAsParquetFile(filePath.toString)
  }


  before {
    hdfsBefore()
    val sc = new SparkContext("local", "DirIndexParquetTest")
    sqlContext = new SQLContext(sc)
  }

  after {
    sqlContext.sparkContext.stop()
  }
}
