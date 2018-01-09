package org.unisk.lacci

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 北京位置基础库任务
  *
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object SparkLacciQuery {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: SparkLacciQuery <province> <daytime>")
      System.exit(1)
    }

    val province = args(0)
    val daytime = args(1)

    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName("lacci_" + province + "_" + daytime)
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("spark.files.ignoreCorruptFiles", value = true)
      .config("spark.sql.autoBroadcastJoinThreshold", 524288000)
      .config("hive.groupby.orderby.position.alias", value = true)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

    val uri_temp_df = spark.read.parquet(s"/sunyj/out/uris/business=*/daytime=$daytime/*.parquet")
    uri_temp_df.createOrReplaceTempView("uri_temp")

    val df = sql(
      s"""
        |select
        | lac,
        | ci
        |from
        | uri_temp
        |group by lac, ci
      """.stripMargin)

    df.write.mode(SaveMode.Append).parquet(s"hdfs://master1:9000/sunyj/out/lacci/$daytime")

    spark.stop()
  }

}
