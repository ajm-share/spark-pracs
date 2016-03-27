
package com

import org.apache.spark.{SparkConf, SparkContext}

object Starter {

  def main(args : Array[String]): Unit = {

  		val sc = new SparkContext(new SparkConf().setAppName("Spark-Airline").setMaster("local[*]"))

  		val orgDataset = sc.textFile("file:///home/cloudera/datasets/2008.csv")

			val line = sc.parallelize(List("hello how r u "))

      line.flatMap(_.split(" ")).map(x => (x(0),1)).reduceByKey(_+_)

  		val dataCount =  orgDataset.count()

  		println("Total Count : "+ dataCount)

  	}

}  	