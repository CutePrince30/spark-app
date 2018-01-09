package org.unisk.keyword

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object SparkUri2Es {

  val conf: SparkConf = new SparkConf().setAppName("spark-es").setMaster("spark://master:7077")
  conf.set("es.index.auto.create", "true")

  val sc = new SparkContext(conf)

  val spark = SparkSession.builder().getOrCreate()


  import org.elasticsearch.spark._

  val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
  val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

  sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs", Map("es.nodes" -> "192.168.59.100", "es.index.auto.create" -> "true"))

  var iiimDF = spark.read.parquet("file:///home/hadoop/data/baidu_out/*.parquet").select("msisdn", "uri")

  val iiimRDD: RDD[Map[String, String]] = iiimDF.rdd.map(row => {
    Map("msisdn" -> row.getString(0), "uri" -> row.getString(1))
  })

  val writeConfig = Map(
    "es.nodes" -> "10.245.5.31,10.245.5.32,10.245.5.33",
    "es.index.auto.create" -> "false",
    "es.batch.size.entries" -> "30000",
    "es.batch.write.retry.count" -> "20",
    "es.batch.write.retry.wait" -> "30s"
  )
  EsSpark.saveToEs(iiimRDD, "xdr/uri", writeConfig)

//  case class Trip(departure: String, arrival: String)
//
//  val upcomingTrip = Trip("OTP", "SFO")
//  val lastWeekTrip = Trip("MUC", "OTP")
//
//  val rdd: RDD[Trip] = sc.makeRDD(Seq(upcomingTrip, lastWeekTrip))
//  EsSpark.saveToEs(rdd, "spark/docs")

}
