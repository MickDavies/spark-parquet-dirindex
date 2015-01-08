package org.apache.spark.sql.parquet

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.hdfs.MiniDFSCluster

/**
 * A trait used to support tests requiring HDFS
 *
 * Note HDFS cluster initialisation takes a couple of seconds
 */
trait HDFSTester {
    val hadoopConf = new Configuration
    var hdfsURI:String = null
    var hdfsCluster:MiniDFSCluster = null
    var hdfs:FileSystem = null
    var basePath:Path = null

  def hdfsBeforeAll() {
    val testDir = new File("./target/hdfs/testDir")
    FileUtil.fullyDelete(testDir)
    hadoopConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, testDir.getAbsolutePath)
    val builder = new MiniDFSCluster.Builder(hadoopConf)
    hdfsCluster  = builder.build()
    hdfsURI = "hdfs://localhost:"+ hdfsCluster.getNameNodePort + "/"
    basePath = new Path(hdfsURI + "base")
    hdfs = FileSystem.get(new java.net.URI(hdfsURI), hadoopConf)
  }

  def hdfsAfterAll() {
    hdfsCluster.shutdown()
  }

  def hdfsBefore() {
    assert(hdfs.mkdirs(basePath))
  }

  def hdfsAfter() {
    assert(hdfs.delete(basePath, true))
  }

  def URI(dir: String) = hdfsURI + dir

  def touchFile(d: Path, f: String) {
    val fp = new Path(d, f)
    assert(hdfs.createNewFile(fp))
  }

  def mkDir(d: String): Path = {
    val sp = new Path(basePath, d)
    assert(hdfs.mkdirs(sp))
    sp
  }

  def allFiles(path: Path) = FileUtilities.allFiles(hdfs, path)
}
