package org.apache.spark.sql.parquet

import java.util.{List => JList}

import _root_.parquet.hadoop.ParquetInputFormat
import _root_.parquet.hadoop.util.ContextUtil
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs._
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.{InputSplit, Job, JobContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{NewHadoopPartition, RDD}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Row, _}
import org.apache.spark.sql.catalyst.types.{IntegerType, StructType}
import org.apache.spark.sql.sources.CatalystScan
import org.apache.spark.{Logging, Partition => SparkPartition}

import scala.annotation.tailrec
import scala.collection.JavaConversions._


private[parquet] case class DirectoryStructurePartition(partitionValues: Seq[Any], files: Seq[FileStatus])

/**
 * Based on the initial path within the FileSystem find all sets of sub-dirs that match the size of directory types
 * then create a list of DirectoryStructurePartition from the converted values for these types derived from the dir names
 * and the files beneath those directories
 */

private[parquet] object FileUtilities {
  def allFiles(fs: FileSystem, path: Path): Seq[LocatedFileStatus] = {
    @tailrec
    def toList(it: RemoteIterator[LocatedFileStatus], acc: List[LocatedFileStatus]): List[LocatedFileStatus] = {
      if (it.hasNext) toList(it, it.next :: acc)
      else acc.reverse
    }
    val it = fs.listFiles(path, true)
    toList(it, Nil)
  }
}


private[parquet] object DirectoryStructure {

  /** TODO define complete set of supported types for dirindex
    * How should escaping be handled
    */
  def toType(s: String, dt: DataType): Any = {
    if ("NULL".equals(s)) null
    else
      try {
        dt match {
          case StringType => s
          case BooleanType => s.toBoolean
          case DoubleType => s.toDouble
          case FloatType => s.toFloat
          case ByteType => s.toByte
          case IntegerType => s.toInt
          case LongType => s.toLong
          case ShortType => s.toShort
        }
      } catch {
        case e: Exception => throw new IllegalArgumentException(s"dir name $s cannot be converted to type $dt", e)
      }
  }

  def discoverPartitions(fs:FileSystem, initialPath:String, directoryColumns: Seq[(String, DataType)]):Seq[DirectoryStructurePartition] = {

    def childDirs(path: Path): Seq[Path] = fs.listStatus(path).filter(s => s.isDirectory).map(_.getPath)


    def files(path:Path):Seq[FileStatus] = {
      FileUtilities.allFiles(fs, path)
        .filter(s => s.isFile)
        .filterNot(_.getPath.getName.startsWith("_"))
        .filterNot(_.getPath.getName.startsWith("."))
    }
    def partitionValue(path: Path, directoryColumn: (String, DataType)): Any = toType(path.getName, directoryColumn._2)

    def go(partitionValues:List[Any], path:Path, remainingDirectoryColumns: Seq[(String, DataType)]):Seq[DirectoryStructurePartition] = remainingDirectoryColumns match {
      case Nil => List(DirectoryStructurePartition(partitionValues.reverse, files(path)))
      case remainingDirectoryColumn :: remainingDirectoryColumns1 =>
        childDirs(path).flatMap(child => go(partitionValue(child, remainingDirectoryColumn)::partitionValues, child, remainingDirectoryColumns1))
    }

    go(List[Any](), new Path(initialPath), directoryColumns)
  }

}


/**
 * Trial implementation of directory based indexing for parquet files see
 * [[org.apache.spark.sql.parquet.dirindex.DefaultSource]] for details
 */
@DeveloperApi
case class DirIndexParquetRelation(initialPath: String, directoryColumns: Seq[(String, DataType)])(@transient val sqlContext: SQLContext)
  extends CatalystScan with Logging {
  @transient
  // Minor Hack: scala doesn't seem to respect @transient for vals declared via extraction
  private val partitions: Seq[DirectoryStructurePartition] =
    DirectoryStructure.discoverPartitions(
      FileSystem.get(new java.net.URI(initialPath), sqlContext.sparkContext.hadoopConfiguration), initialPath, directoryColumns)
  override val sizeInBytes = partitions.flatMap(_.files).map(_.getLen).sum
  override val schema = StructType.fromAttributes(// TODO schema should be derived from sum of all schemas - requires change to parquet processing
    ParquetTypesConverter.readSchemaFromFile(
      partitions.head.files.head.getPath,
      Some(sparkContext.hadoopConfiguration),
      sqlContext.isParquetBinaryAsString))

  override def buildScan(output: Seq[Attribute], predicates: Seq[Expression]): RDD[Row] = {
    // This is mostly a hack so that we can use the existing parquet filter code.
    val requiredColumns = output.map(_.name)

    val job = new Job(sparkContext.hadoopConfiguration)
    ParquetInputFormat.setReadSupportClass(job, classOf[RowReadSupport])
    val jobConf: Configuration = ContextUtil.getConfiguration(job)

    val requestedSchema = StructType(requiredColumns.map(schema(_)))

    val partitionKeys = directoryColumns.map(_._1)
    val partitionKeySet = partitionKeys.toSet
    val rawPredicate =
      predicates
        .filter(_.references.map(_.name).toSet.subsetOf(partitionKeySet))
        .reduceOption(And)
        .getOrElse(Literal(true))

    // Translate the predicate so that it reads from the information derived from the
    // folder structure
    val castedPredicate = rawPredicate transform {
      case a: AttributeReference =>
        val idx = partitionKeys.indexWhere(a.name == _)
        BoundReference(idx, IntegerType, nullable = true)
    }

    val inputData = new GenericMutableRow(partitionKeys.size)
    val pruningCondition = InterpretedPredicate(castedPredicate)

    val selectedPartitions =
      if (partitionKeys.nonEmpty && predicates.nonEmpty) {
        partitions.filter { entry =>
          for (col <- 0 until partitionKeys.size) inputData(col) = entry.partitionValues(col)
          pruningCondition(inputData)
        }
      } else {
        partitions
      }

    val fs = FileSystem.get(new java.net.URI(initialPath), sparkContext.hadoopConfiguration)
    val selectedFiles = selectedPartitions.flatMap(_.files).map(f => fs.makeQualified(f.getPath))

    // FileInputFormat cannot handle empty lists.
    if (selectedFiles.nonEmpty) {
      org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job, selectedFiles: _*)
    }

    // Push down filters when possible
    predicates
      .reduceOption(And)
      .flatMap(ParquetFilters.createFilter)
      .filter(_ => sqlContext.parquetFilterPushDown)
      .foreach(ParquetInputFormat.setFilterPredicate(jobConf, _))

    def percentRead = selectedPartitions.size.toDouble / partitions.size.toDouble * 100
    logInfo(s"Reading $percentRead% of $initialPath partitions")

    // Store both requested and original schema in `Configuration`
    jobConf.set(
      RowReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      ParquetTypesConverter.convertToString(requestedSchema.toAttributes))
    jobConf.set(
      RowWriteSupport.SPARK_ROW_SCHEMA,
      ParquetTypesConverter.convertToString(schema.toAttributes))

    // Tell FilteringParquetRowInputFormat whether it's okay to cache Parquet and FS metadata
    val useCache = sqlContext.getConf(SQLConf.PARQUET_CACHE_METADATA, "true").toBoolean
    jobConf.set(SQLConf.PARQUET_CACHE_METADATA, useCache.toString)

    val baseRDD =
      new org.apache.spark.rdd.NewHadoopRDD(
        sparkContext,
        classOf[FilteringParquetRowInputFormat],
        classOf[Void],
        classOf[Row],
        jobConf) {
        val cacheMetadata = useCache

        @transient
        val cachedStatus = selectedPartitions.flatMap(_.files).toList

        // Overridden so we can inject our own cached files statuses.
        override def getPartitions: Array[SparkPartition] = {
          val inputFormat =
            if (cacheMetadata) {
              new FilteringParquetRowInputFormat {
                override def listStatus(jobContext: JobContext): JList[FileStatus] = cachedStatus
              }
            } else {
              new FilteringParquetRowInputFormat
            }

          inputFormat match {
            case configurable: Configurable =>
              configurable.setConf(getConf)
            case _ =>
          }
          val jobContext = newJobContext(getConf, jobId)
          val rawSplits = inputFormat.getSplits(jobContext).toArray
          val result = new Array[SparkPartition](rawSplits.size)
          for (i <- 0 until rawSplits.size) {
            result(i) =
              new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
          }
          result
        }
      }
    baseRDD.map(_._2)
  }

  def sparkContext = sqlContext.sparkContext
}