package com.klyk.lab3

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by klyk on 7/21/15.
 */
object EntityResolution {

  def main(args: Array[String]) {
    if (args.length < 3) {
      println("Usage: [sparkmaster] [inputGoogleFile] [inputAmazonFile] [outputdirectory]")
      System.exit(1)
    }
    val master = args(0)
    val inputGoogleFile = args(1)
    val inputAmazonFile = args(2)
    val outputFile = args(3)

    val conf = new SparkConf().setMaster(master).setAppName("EntityResolution")
    val sc = new SparkContext(conf)


    //Define StopWords or Read from file
    val stopwords = sc.parallelize(List("the","a", "is", "to"," "))
    val googleCSV = sc.textFile(inputGoogleFile)

    //Convert into Pair RDDs ("id", "name/title description manufacturer")
    val googleRDD = googleCSV
      .map(line=> line.split(","))
      .map(elem => (elem(0).replace("\"",""),"%s %s %s".format(elem(1).replace("\"",""),elem(2).replace("\"",""),elem(3).replace("\"",""))))
      .cache()

    val amazonCSV = sc.textFile(inputAmazonFile)

    val amazonRDD = amazonCSV
      .map(line=> line.split(","))
      .map(elem => (elem(0).replace("\"",""),"%s %s %s".format(elem(1).replace("\"",""),elem(2).replace("\"",""),elem(3).replace("\"",""))))


    //Tokenize and remove stopwords

    val googleTokens = googleRDD
      .flatMap(elem => elem._2.split("\W+"))
      .subtract(stopwords)
      .cache()

    val amazonTokens = amazonRDD
      .flatMap(elem => elem._2.split("\W+"))
      .subtract(stopwords)
      .cache()

    //Amazon Record with the most tokens
    val item = amazonRDD
      .mapValues(item => item.split("\W+").length)
      .map(item => item.swap)
      .sortByKey(false)
      .map(item => item.swap)
      .first()

  }




}
