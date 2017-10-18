import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

object PageRankGraphxScala {
  def main(args: Array[String]): Unit = {
  val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    
    val graph = GraphLoader.edgeListFile(sc, "/hadoopfiles/parsedfile")
    val ranks = graph.pageRank(0.0001).vertices
    println(ranks.collect().mkString("\n"))
  
    spark.stop()
  }
}
