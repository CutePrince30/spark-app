package org.unisk.phoneinfo

import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * @author sunyunjie (jaysunyun_361@163.com)
  */
trait TourHelper {

  def getSparkContext(args: Array[String]): SparkContext = {
    getSparkSession(args).sparkContext
  }

  def getSparkSession(args: Array[String]): SparkSession = {
    val uri: String = args.headOption.getOrElse("mongodb://slave1/xdr.phone_codes")

    val conf = new SparkConf()
      .setMaster("spark://master:7077")
      .setAppName("MongoSparkConnectorTour")
      .set("spark.app.id", "MongoSparkConnectorTour")
      .set("spark.mongodb.input.uri", uri)
      .set("spark.mongodb.output.uri", uri)

    val session = SparkSession.builder().config(conf).getOrCreate()
//    MongoConnector(session.sparkContext).withDatabaseDo(WriteConfig(session), { db => db.drop() })
    session
  }

}
