package org.unisk.test

import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object ReadMongoTest {

  val spark = SparkSession
    .builder()
    .master("yarn")
    .appName("imei_match")
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
    .config("spark.files.ignoreCorruptFiles", value = true)
    .config("spark.sql.autoBroadcastJoinThreshold", 524288000)
    .config("hive.groupby.orderby.position.alias", value = true)
    .enableHiveSupport()
    .getOrCreate()

  import com.mongodb.spark._

  val readConfig = ReadConfig(Map(
    "uri" -> "mongodb://10.245.5.36:27017,10.245.5.35:27017,10.245.5.34:27017/?replicaSet=test&readPreference=secondaryPreferred",
    "database" -> "xdr",
    "collection" -> "phone_codes"
  ))

  val phoneCodesDF = MongoSpark.load(spark, readConfig).select("_id", "imei").withColumnRenamed("_id", "msisdn")

  val imeiDF = spark.read.csv("/sunyj/test/imei.txt").withColumnRenamed("_c0", "imei")

  val resultDF = phoneCodesDF.join(imeiDF, Seq("imei"), "inner")

  resultDF.write.mode("append").csv("hdfs://master1:9000/sunyj/test/imei_match")

  spark.stop()
}
