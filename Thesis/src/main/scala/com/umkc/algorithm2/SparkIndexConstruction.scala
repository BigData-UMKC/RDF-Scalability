package com.umkc.algorithm2

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.Logger
import org.apache.log4j.Level
/**
 * Created by hastimal on 11/6/2015.
 */
object SparkIndexConstruction {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //For windows environment
    System.setProperty("hadoop.home.dir", "F:\\winutils")

    var conf =new SparkConf().setAppName("SparkIndexConstruction").set("spark.executor.memory", "4g").setMaster("local[*]")
    var sc = new SparkContext(conf)

    //Using graphX example, assuning a property graph
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array(
        (3L, ("rxin", "student")),
        (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")),
        (2L, ("istoica", "prof")),
        (4L, ("peter", "student")),
        (0L, ("John Doe", "Missing")))).cache()
    // Create an RDD for edges
    System.out.println("##########Vertex RDD as users##########")
    users.collect.foreach(println)

    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(
        Edge(3L, 7L, "collab"),
        Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"),
        Edge(5L, 7L, "pi"),
        Edge(4L, 0L, "student"))).cache()
    // Edge(5L, 0L, "colleague"))).cache()  //Removed edge between 5 and 0 node to get two different connected graphs
    System.out.println("##########Edge RDD as users##########")
    relationships.collect.foreach(println)

    // Define a default user in case there are relationship with missing user
    //val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val propGraph = Graph(users, relationships)
    System.out.println("##########Printing raw graph##########")
    propGraph.triplets.foreach(println(_))
    System.out.println("##########Printing raw graph with connected components##########")
    val cc=propGraph.connectedComponents()
    cc.triplets.foreach(println(_))
//    System.out.println("##########Printing raw graph with connected components verices##########")
//    val ccV=propGraph.connectedComponents().vertices
//    ccV.foreach(println(_))





  }
}
