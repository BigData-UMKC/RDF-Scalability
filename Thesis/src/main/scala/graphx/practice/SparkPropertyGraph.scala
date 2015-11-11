package graphx.practice

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by hastimal on 11/9/2015.
 */
object SparkPropertyGraph {
      def main(args: Array[String]) {
    //https://spark.apache.org/docs/0.9.0/graphx-programming-guide.html
    System.setProperty("hadoop.home.dir", "F:\\winutils")
        import org.apache.log4j.Logger
        import org.apache.log4j.Level
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

    //St config for Spark
    val conf = new SparkConf().setAppName("SparkPropertyGraph").setMaster("local[*]").set("spark.executor.memory", "3g")
    val sc = new SparkContext(conf)

    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array(
        (3L, ("rxin", "student")),
        (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")),
        (2L, ("istoica", "prof")),
        (4L, ("peter", "student")))).cache()
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
       // Edge(5L, 0L, "colleague"))).cache()
        System.out.println("##########Edge RDD as users##########")
        relationships.collect.foreach(println)
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)
        System.out.println("##########Printing raw graph##########")
        graph.triplets.foreach(println(_))
        System.out.println("##########Printing raw graph with connected components##########")
        graph.connectedComponents().triplets.foreach(println(_))
        System.out.println("##########Printing all roles##########")
    //Printing all nodes attributes in the form of role
    graph.triplets.map(
      triplet => triplet.srcId +" is "+triplet.srcAttr._1 + " has role "  + triplet.srcAttr._2
    ).collect.distinct.foreach(println(_))
    graph.triplets.map(
      triplet => triplet.dstId +" is "+triplet.dstAttr._1 + " has role "  + triplet.dstAttr._2
    ).collect.distinct.foreach(println(_))

        System.out.println("##########read property of graph##########")
    // Notice that there is a user 0 (for which we have no information) connected to users
    // 4 (peter) and 5 (franklin).
    graph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))


        System.out.println("##########subgraph with removing missing##########")
    // Remove missing vertices as well as the edges to connected to them
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    // The valid subgraph will disconnect users 4 and 5 by removing user 0
    validGraph.vertices.collect.foreach(println(_))

        System.out.println("##########Edge RDD as users after missing##########")
    validGraph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))
        System.out.println("##########Edge RDD as users after missing##########")
    // Run Connected Components
    val ccGraph = graph.connectedComponents() // No longer contains missing field
    // Remove missing vertices as well as the edges to connected to them
    val validGraph2 = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    // Restrict the answer to the valid subgraph
    val validCCGraph = ccGraph.mask(validGraph2)
    validCCGraph.triplets.map(
      triplet => triplet.toTuple._1._1+ " is the " + triplet.attr + " of " + triplet.toTuple._2._1
    ).collect.foreach(println(_))
  System.out.println("##########Here is connected components##########")
    // Run Connected Components
    val ccGraph1 = graph.connectedComponents() // No longer contains missing field
        ccGraph1.triplets.map(
          triplet => triplet.toTuple._1._1+ " is the " + triplet.attr + " of " + triplet.toTuple._2._1
        ).collect.foreach(println(_))



        // Load the graph as in the PageRank example
        val graph2 = GraphLoader.edgeListFile(sc, "src/main/resources/inputData/followers.txt")
        // Find the connected components
       // graph2.connectedComponents().foreach(println)
        val cc = graph2.connectedComponents().vertices
        // Join the connected components with the usernames
        val users2 = sc.textFile("src/main/resources/inputData/users.txt").map { line =>
          val fields = line.split(",")
          (fields(0).toLong, fields(1))
        }
        users2.collect().foreach(println)
        val ccByUsername = users2.join(cc).map {
          case (id, (username, cc)) => (username, cc)
        }
        // Print the result
        println(ccByUsername.collect().mkString("\n"))
  }
}