package org.apache.spark.sql.parquet

import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec, Matchers}

class DirectoryStructureTest extends FlatSpec with Matchers with BeforeAndAfter with BeforeAndAfterAll with HDFSTester {

  "FileUtilities.allFiles" should "find all files" in {
    val d = createDirAndTouchFile("d")
    createDirAndTouchFile("d/sub")
    FileUtilities.allFiles(hdfs, d).map(_.getPath) should be {
      List(path("d/f"), path("d/sub/f"))
    }
  }

  "A DirectoryStructure" should "discover handle empty directory" in {
    val partitions = DirectoryStructure.discoverPartitions(hdfs, basePath.toString, List(("c1", StringType)))
    partitions should be (Nil)
  }

  it should "discover single dir" in {
    val c1v1 = createDirAndTouchFile("c1v1")
    val partitions = DirectoryStructure.discoverPartitions(hdfs, basePath.toString, List(("c1", StringType)))
    partitions should be (List(DirectoryStructurePartition(List("c1v1"), hdfs.listStatus(c1v1))))
  }

  it should "discover files in two dirs" in {
    val c1v1 = createDirAndTouchFile("c1v1")
    val c1v2 = createDirAndTouchFile("c1v2")
    val partitions = DirectoryStructure.discoverPartitions(hdfs, basePath.toString, List(("c1", StringType)))
    partitions should be (List(
      DirectoryStructurePartition(List("c1v1"), allFiles(c1v1)),
      DirectoryStructurePartition(List("c1v2"), allFiles(c1v2))))
  }


  it should "discover files in sub dirs" in {
    val c1v1 = createDirAndTouchFile("c1v1")
    createDirAndTouchFile("c1v1/zsub")
    val partitions = DirectoryStructure.discoverPartitions(hdfs, basePath.toString, List(("c1", StringType)))
    partitions should be (List(
      DirectoryStructurePartition(List("c1v1"), allFiles(c1v1))))
  }

  it should "support multiple cols" in {
    mkDir("c1v1")
    val c2v1 = createDirAndTouchFile("c1v1/c2v1")

    val partitions = DirectoryStructure.discoverPartitions(hdfs, basePath.toString, List(("c1", StringType), ("c2", StringType)))
    partitions should be(List(DirectoryStructurePartition(List("c1v1", "c2v1"), allFiles(c2v1))))
  }

  it should "discover support multiple cols with multiple sub directories" in {
    mkDir("c1v1")
    val c2v1 = createDirAndTouchFile("c1v1/c2v1")
    val c2v2 = createDirAndTouchFile("c1v1/c2v2")

    val partitions = DirectoryStructure.discoverPartitions(hdfs, basePath.toString, List(("c1", StringType), ("c2", StringType)))
    partitions should be (List(
      DirectoryStructurePartition(List("c1v1", "c2v1"), allFiles(c2v1)),
      DirectoryStructurePartition(List("c1v1", "c2v2"), allFiles(c2v2))))
  }


  def createDirAndTouchFile(d: String): Path = {
    val c1v1: Path = mkDir(d)
    touchFile(c1v1, "f")
    c1v1
  }


  before {
    hdfsBefore()
  }
}