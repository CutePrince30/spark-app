package org.unisk.iiim

import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * 向mongo更新北京四码基础库任务
  *
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object UpdatePhoneCodes {

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      System.err.println("Usage: UpdatePhoneCodes <daytime>")
      System.exit(1)
    }

    val daytime = args(0)

    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName(s"update_phone_codes_$daytime")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("spark.files.ignoreCorruptFiles", value = true)
      .config("spark.sql.autoBroadcastJoinThreshold", 524288000)
      .config("hive.groupby.orderby.position.alias", value = true)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql
    import com.mongodb.spark._

    val writeConfig = WriteConfig(Map("uri" -> "mongodb://10.245.5.36:27017",
      "database" -> "xdr", "collection" -> "phone_codes", "replaceDocument" -> "false"))

    var iiimDailyDF = spark.read.parquet(s"/sunyj/out/iiim/$daytime/*.parquet").withColumnRenamed("msisdn", "_id")
      .persist(StorageLevel.MEMORY_ONLY_SER)

    // 过滤出带有idfa的，进行更新
    val idfaDF = iiimDailyDF.filter("idfa != ''").select("_id", "imei", "idfa")
    MongoSpark.save(idfaDF, writeConfig)

    val imeiDF = iiimDailyDF.filter("idfa == ''").select("_id", "imei", "imsi")
    MongoSpark.save(imeiDF, writeConfig)

    spark.stop()
  }

}
