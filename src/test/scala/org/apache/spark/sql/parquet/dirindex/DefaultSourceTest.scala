package org.apache.spark.sql.parquet.dirindex


import org.apache.spark.sql._
import org.scalatest.{FlatSpec, Matchers}


class DefaultSourceTest extends FlatSpec with Matchers {
  "A DefaultSource" should "extract a single String column" in {
    DefaultSource.extractColumns(Map("col1" -> "c1:String"), 1) should be(List(("c1", StringType)))
  }

  it should "extract a single Int column" in {
    DefaultSource.extractColumns(Map("col1" -> "c1:Integer"), 1) should be(List(("c1", IntegerType)))
  }

  it should "extract a two columns" in {
    DefaultSource.extractColumns(Map("col1" -> "c1:String", "col2" -> "c2:Integer"), 1) should
      be(List(("c1", StringType), ("c2", IntegerType)))
  }

  it should "raise an exception for a bad type" in {
    intercept[java.lang.IllegalArgumentException] {
      DefaultSource.extractColumns(Map("col1" -> "c1:Bad"), 1)
    }.getMessage should include("Unsupported dataType: BadType")
  }
}
