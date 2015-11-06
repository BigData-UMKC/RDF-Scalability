package com.umkc

/**
 * Created by Venu on 10/26/15.
 */

import org.apache.spark.{SparkConf, SparkContext}
object SparkRDF {

  def main(args: Array[String]) {
    //For windows environment
    System.setProperty("hadoop.home.dir", "F:\\winutils")

    var conf =new SparkConf().setAppName("RDF Syntax Processing").setMaster("local")

    var sc = new SparkContext(conf)

    //Step-1
    var inputFile = sc.textFile("src/main/resources/rdf.nt")

    //Step-2
    var rdfTuples = inputFile.map(line => line)
   // rdfTuples.collect.foreach(println)
    //Step-3
    var subjects = inputFile.map(line =>line.split(" ")(0))
    //subjects.collect.foreach(println)
    //Step-4
    var objects = inputFile.map(line => line.split(" ")(2))
    //objects.collect.foreach(println)
    //Step-5
    var distinctSubObj = subjects.union(objects).distinct()
   // distinctSubObj.collect.foreach(println)
    var numberOfPartitions = distinctSubObj.partitions.size
    //Step-6,7,8
    var datarange = Array((0,99),(100,199),(200,299))

    var datarangePartitions = sc.parallelize(datarange,1)

    //Step-9
    var broadCastVar = sc.broadcast(datarange)

    //Step-10
    //var entityIDMapping = distinctSubObj.mapPartitionsWithIndex(datarange,)











  }

}
