name := "spark-parquet-dirindex"

version := "1.0"


libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.2.0"  excludeAll(
  ExclusionRule(organization = "javax.servlet"),
  ExclusionRule(organization = "org.apache.hadoop")
  )

libraryDependencies += "org.apache.spark" % "spark-catalyst_2.10" % "1.2.0" excludeAll(
  ExclusionRule(organization = "javax.servlet"),
  ExclusionRule(organization = "org.apache.hadoop")
  )

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.4.0"  excludeAll(
  ExclusionRule(organization = "javax.servlet")
  )

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.3"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.4.0" % "test" classifier "tests"  excludeAll(
  ExclusionRule(organization = "javax.servlet")
  )

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.4.0" % "test"  excludeAll(
  ExclusionRule(organization = "javax.servlet")
  )

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.4.0" % "test" classifier "tests"   excludeAll(
  ExclusionRule(organization = "javax.servlet")
  )




