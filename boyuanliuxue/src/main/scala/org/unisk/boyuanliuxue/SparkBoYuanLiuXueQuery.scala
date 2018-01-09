package org.unisk.boyuanliuxue

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 博远留学任务
  *
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object SparkBoYuanLiuXueQuery {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: SparkBoYuanLiuXueQuery <province> <daytime>")
      System.exit(1)
    }

    val province = args(0)
    val daytime = args(1)

    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName("boyuanliuxue_" + province + "_" + daytime)
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("spark.files.ignoreCorruptFiles", value = true)
      .config("spark.sql.autoBroadcastJoinThreshold", 524288000)
      .config("hive.groupby.orderby.position.alias", value = true)
      .enableHiveSupport()
      .getOrCreate()


    import spark.sql

    val boyuanliuxue_common_df = spark.read.parquet(s"/sunyj/out/uris/business=boyuan_liuxue/daytime=$daytime/*.parquet")
    boyuanliuxue_common_df.createOrReplaceTempView("boyuanliuxue_common")

    val baidu_boyuanliuxue_df = spark.read.parquet(s"/sunyj/out/uris/business=baidu/daytime=$daytime/*.parquet")
    baidu_boyuanliuxue_df.createOrReplaceTempView("baidu_boyuanliuxue")
    val baidu_keywords = "%E8%AE%BE%E8%AE%A1+%E7%95%99%E5%AD%A6%09|%E7%94%B5%E5%BD%B1+%E7%95%99%E5%AD%A6%09|%E7%95%99%E5%AD%A6+%E4%BD%9C%E5%93%81%E9%9B%86%09|%E8%89%BA%E6%9C%AF+%E7%95%99%E5%AD%A6%09|%E8%A7%86%E8%A7%89+%E7%95%99%E5%AD%A6%09|%E5%BD%B1%E8%A7%86+%E7%95%99%E5%AD%A6%09|3D%E8%A3%85%E7%BD%AE+%E7%95%99%E5%AD%A6%09|%E4%BA%A4%E4%BA%92+%E7%95%99%E5%AD%A6%09|%E5%8A%A8%E7%94%BB+%E7%95%99%E5%AD%A6%09|%E5%87%BA%E5%9B%BD+%E7%BE%8E%E6%9C%AF%09|%E7%BE%8E%E6%9C%AF+%E7%95%99%E5%AD%A6%09|%E5%BB%BA%E7%AD%91+%E7%95%99%E5%AD%A6%09|%E6%8F%92%E7%94%BB+%E7%95%99%E5%AD%A6%09|%E7%BE%8E%E6%9C%AF+%E5%87%BA%E5%9B%BD%09"

    val boyuanliuxue_out = sql(
      s"""
        |select
        | keyword,
        | concat_ws(',', collect_set(msisdn)) as msisdn,
        | count(keyword) as pv,
        | size(collect_set(msisdn)) as uv
        |from
        | boyuanliuxue_common
        |group by keyword
        |
        |union all
        |
        |select
        | keyword,
        | concat_ws(',', collect_set(msisdn)) as msisdn,
        | count(keyword) as pv,
        | size(collect_set(msisdn)) as uv
        |from
        | (select
        |   msisdn,
        |   regexp_extract(concat(uri, refer), '($baidu_keywords)', 0) as keyword
        |  from baidu_boyuanliuxue) a
        |where
        | a.keyword != ''
        |group by a.keyword
      """.stripMargin)

    boyuanliuxue_out.write.option("sep", "\t").mode(SaveMode.Append)
      .csv(s"hdfs://master1:9000/sunyj/out/boyuanliuxue/$daytime")

    spark.stop()
  }

}

