package org.apache.spark.sql.parquet

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.hdfs.MiniDFSCluster

/**
 * A trait used to support tests requiring HDFS
 */
trait HDFSTester {
    val hadoopConf = new Configuration
    var hdfsURI:String = null
    var hdfs:FileSystem = null
    var basePath:Path = null


  def hdfsBefore() {
    val testDir = File.createTempFile("./target/hdfs", "sparksql")
    FileUtil.fullyDelete(testDir)
    assert(testDir.mkdirs())
    testDir.deleteOnExit()
    hdfsURI = testDir.getCanonicalPath
    basePath = new Path("file:" + hdfsURI)
    hdfs = FileSystem.get(new java.net.URI(hdfsURI), hadoopConf)
  }

  def URI(dir: String) = hdfsURI + dir

  def touchFile(d: Path, f: String) {
    val fp = new Path(d, f)
    assert(hdfs.createNewFile(fp))
  }

  def mkDir(d: String): Path = {
    val sp = path(d)
    assert(hdfs.mkdirs(sp))
    sp
  }

  def path(f:String) = new Path(basePath, f)

  def allFiles(path: Path) = FileUtilities.allFiles(hdfs, path)
}
