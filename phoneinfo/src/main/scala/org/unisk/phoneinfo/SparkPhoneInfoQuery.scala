package org.unisk.phoneinfo

import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.sql.SparkSession
import org.bson.Document

/**
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object SparkPhoneInfoQuery extends TourHelper {

  def main(args: Array[String]): Unit = {
    val sc = getSparkContext(args)

    import com.mongodb.spark._

//    val docs =
//      """
//        |{"name": "Bilbo Baggins", "age": 50}
//        |{"name": "Gandalf", "age": 1000}
//        |{"name": "Thorin", "age": 195}
//        |{"name": "Balin", "age": 178}
//        |{"name": "Kíli", "age": 77}
//        |{"name": "Dwalin", "age": 169}
//        |{"name": "Óin", "age": 167}
//        |{"name": "Glóin", "age": 158}
//        |{"name": "Fíli", "age": 82}
//        |{"name": "Bombur"}""".trim.stripMargin.split("[\\r\\n]+").toSeq
//
//    // 保存数据至mongo
//    MongoSpark.save(sc.parallelize(docs.map(Document.parse)))




    // Create SparkSession
    val spark = SparkSession.builder().getOrCreate()

    val readConfig = ReadConfig(Map("uri" -> "mongodb://192.168.59.101:27017", "database" -> "local", "collection" -> "startup_log"))
    val customDF = com.mongodb.spark.MongoSpark.load(spark, readConfig)
    customDF.show()

    var iiimDF = spark.read.parquet("file:///home/hadoop/data/iiim.snappy.parquet")
    iiimDF = iiimDF.filter("msisdn regexp '^(86)?1[^(0|1|2)][0-9]{9}$'")
    iiimDF = iiimDF.withColumnRenamed("msisdn", "_id")
    val writeConfig = WriteConfig(Map("uri" -> "mongodb://192.168.59.101:27017",
      "database" -> "xdr", "collection" -> "phone", "replaceDocument" -> "false"))
    com.mongodb.spark.MongoSpark.save(iiimDF, writeConfig)

    var tempDS = spark.createDataset("""{"_id": "13000000000", "imei": "1111111111", "imsi": "111111111", "idfa": ""}""" :: Nil)
    val tempDF = spark.read.json(tempDS)
    com.mongodb.spark.MongoSpark.save(tempDF, writeConfig)



    val loc_df = spark.read.parquet("/sunyj/out/people_loc/province=guangdong/daytime=20171212/*.parquet")

    MongoSpark.save(loc_df)

    // Import the SQL helper
    val df = MongoSpark.load(spark)
    df.printSchema()

    df.show()

  }

}
