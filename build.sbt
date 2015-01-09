name := "spark-parquet-dirindex"

version := "0.1"


libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "provided"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.2.0" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.2.0" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.2.0" % "test" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.2.0" % "test"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.2.0" % "test" classifier "tests"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.3"  % "test"


