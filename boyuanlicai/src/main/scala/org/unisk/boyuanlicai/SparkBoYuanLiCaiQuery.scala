package org.unisk.boyuanlicai

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 博远理财任务
  *
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object SparkBoYuanLiCaiQuery {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: SparkBoYuanLiCaiQuery <province> <daytime>")
      System.exit(1)
    }

    val province = args(0)
    val daytime = args(1)

    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName("boyuanlicai_" + province + "_" + daytime)
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("spark.files.ignoreCorruptFiles", value = true)
      .config("spark.sql.autoBroadcastJoinThreshold", 524288000)
      .config("hive.groupby.orderby.position.alias", value = true)
      .enableHiveSupport()
      .getOrCreate()


    import spark.sql

    val boyuanlicai_common_df = spark.read.parquet(s"/sunyj/out/uris/business=boyuan_licai/daytime=$daytime/*.parquet")
    boyuanlicai_common_df.createOrReplaceTempView("boyuanlicai_common")

    val baidu_boyuanlicai_df = spark.read.parquet(s"/sunyj/out/uris/business=baidu/daytime=$daytime/*.parquet")
    baidu_boyuanlicai_df.createOrReplaceTempView("baidu_boyuanlicai")
    val baidu_keywords = "5%E4%B8%87%E5%85%83%E6%8A%95%E8%B5%84%E7%90%86%E8%B4%A2%09|30%E4%B8%87%E5%85%83%E7%90%86%E8%B4%A2%09|%E5%88%A9%E6%81%AF+%E7%90%86%E8%B4%A2%09|%E7%90%86%E8%B4%A2%09|%E4%BA%92%E8%81%94%E7%BD%91+%E7%90%86%E8%B4%A2%09|%E5%B0%8F%E9%A2%9D+%E6%8A%95%E8%B5%84%09|%E5%B0%8F%E9%A2%9D+%E7%90%86%E8%B4%A2%09|%E6%9C%88%E8%96%AA+%E7%90%86%E8%B4%A2%09|%E5%AD%98%E6%AC%BE+%E7%90%86%E8%B4%A2%09|%E6%8A%95%E8%B5%84+%E7%90%86%E8%B4%A2%09|P2P%E7%90%86%E8%B4%A2%09|%E6%83%A0%E5%86%9C%E8%81%9A%E5%AE%9D%09|5%E4%B8%87%E5%85%83%E6%8A%95%E8%B5%84%E7%90%86%E8%B4%A2%09|30%E4%B8%87%E5%85%83%E7%90%86%E8%B4%A2%09|%E5%AD%9810%E4%B8%87%E4%B8%80%E5%B9%B4%E5%88%A9%E6%81%AF%E5%A4%9A%E5%B0%91%09|%E6%8A%95%E8%B5%84+%E7%90%86%E8%B4%A2%09|%E5%AE%9A%E6%9C%9F+%E7%90%86%E8%B4%A2%09|%E5%A6%82%E4%BD%95%E7%90%86%E8%B4%A2%09|%E6%B4%BB%E6%9C%9F+%E7%90%86%E8%B4%A2%09|2000%E5%85%83%E6%8A%95%E8%B5%84%09|10%E4%B8%87%E6%80%8E%E4%B9%88%E7%90%86%E8%B4%A2%09|%E6%8A%95%E8%B5%84%E7%90%86%E8%B4%A2%E9%A1%BE%E9%97%AE%09|%E9%87%91%E8%9E%8D+%E7%90%86%E8%B4%A2%09"

    val boyuanlicai_out = sql(
      s"""
        |select
        | keyword,
        | concat_ws(',', collect_set(msisdn)) as msisdn,
        | count(keyword) as pv,
        | size(collect_set(msisdn)) as uv
        |from
        | boyuanlicai_common
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
        |  from baidu_boyuanlicai) a
        |where
        | a.keyword != ''
        |group by a.keyword
      """.stripMargin)

    boyuanlicai_out.write.option("sep", "\t").mode(SaveMode.Append)
      .csv(s"hdfs://master1:9000/sunyj/out/boyuanlicai/$daytime")

    spark.stop()
  }

}

