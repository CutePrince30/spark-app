package org.unisk.iiim

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 北京四码基础库任务
  *
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object SparkIIIMQuery {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: SparkIIIMQuery <province> <daytime>")
      System.exit(1)
    }

    val province = args(0)
    val daytime = args(1)

    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName("iiim_" + province + "_" + daytime)
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("spark.files.ignoreCorruptFiles", value = true)
      .config("spark.sql.autoBroadcastJoinThreshold", 524288000)
      .config("hive.groupby.orderby.position.alias", value = true)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

    val uri_temp_df = spark.read.parquet(s"/sunyj/out/uris/business=*/daytime=$daytime/*.parquet")
    val loc_temp_df = spark.read.parquet(s"/sunyj/out/people_loc/province=*/daytime=$daytime/*.parquet")

    uri_temp_df.createOrReplaceTempView("uri_temp")
    loc_temp_df.createOrReplaceTempView("loc_temp")

    val df = sql(
      s"""
        |select
        | msisdn,
        | first(imei) as imei,
        | first(imsi) as imsi,
        | collect_set(idfa)[0] as idfa
        |from
        |(select
        | case length(msisdn) when 13 then substr(msisdn, 3, 11) else msisdn end as msisdn,
        | imei,
        | imsi,
        | idfa
        |from
        | uri_temp
        |where msisdn regexp '^(86)?1[^(0|1|2)][0-9]{9}$$'
        |
        |union all
        |
        |select
        | case length(msisdn) when 13 then substr(msisdn, 3, 11) else msisdn end as msisdn,
        | imei,
        | imsi,
        | '' as idfa
        |from
        | loc_temp
        |where msisdn regexp '^(86)?1[^(0|1|2)][0-9]{9}$$'
        |) tmp
        |group by msisdn
      """.stripMargin)

    df.write.mode(SaveMode.Append).parquet(s"hdfs://master1:9000/sunyj/out/iiim/$daytime")

    spark.stop()
  }

}
