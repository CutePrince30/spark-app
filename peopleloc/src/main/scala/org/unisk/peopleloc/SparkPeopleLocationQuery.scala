package org.unisk.peopleloc

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import scala.util.Try

/**
  * 位置任务
  *
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object SparkPeopleLocationQuery {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: SparkPeopleLocationQuery <province> <daytime>")
      System.exit(1)
    }

    val province = args(0)
    val daytime = args(1)

    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName(s"people_loc_${province}_$daytime")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("spark.files.ignoreCorruptFiles", value = true)
      .config("spark.sql.autoBroadcastJoinThreshold", 524288000)
      .config("hive.groupby.orderby.position.alias", value = true)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql
    import spark.implicits._

    val loc_df = spark.sql(
      """
        |select
        | concat_ws('_', lac, ci) as pid,
        | concat_ws('_', lon, lat, province) as ploc
        |from
        | xdr.location
        |where
        | province in ('beijing', 'guangdong', 'jiangsu', 'zhejiang', 'jiangxi', 'gansu') and city = 'all'
      """.stripMargin)

    val loc_info = loc_df.rdd.map(record => record(0) -> record(1)).collectAsMap()

    val loc_bc = spark.sparkContext.broadcast(loc_info)

    val http_df = sql(
      s"""
         |select
         |	msisdn,
         |	imei,
         |	imsi,
         |	from_unixtime(unix_timestamp(starttime, 'yyyy-MM-dd HH:mm:ss'), 'yyyyMMddHHmmss') as starttime,
         |	concat_ws('_', lac_tac, ci) as pid
         |from
         |	xdr.http
         |where
         |	province = "$province" and daytime = "$daytime"
         | and msisdn regexp '^(86)?1[^(0|1|2)][0-9]{9}$$' and imei != ''
      """.stripMargin)

    def merge(iter: Iterator[Row]): Iterator[(String, String, String, String, String, String, String, String, String)] = {
      val loc_map = loc_bc.value
      var set = scala.collection.mutable.Set[(String, String, String, String, String, String, String, String, String)]()
      iter.foreach(row => {
        val pid = row.getString(4)
        if (loc_map.contains(pid)) {
          val loc_value = loc_map.get(pid).mkString.split("_")
          Try {
            set add ((row.getString(0), row.getString(1), row.getString(2), row.getString(3),
              pid.split("_")(0), pid.split("_")(1),
              loc_value(0), loc_value(1), loc_value(2)))
          }
        }
      })
      set.iterator
    }

    var r_df = http_df.mapPartitions(merge).toDF("msisdn", "imei", "imsi", "starttime",
      "lac", "ci", "lon", "lat", "province")

    val result_table_name = s"people_loc_${province}_${daytime}_result"
    r_df.createOrReplaceTempView(result_table_name)

    r_df = spark.sql(
      s"""
         |select
         | msisdn,
         | first(imei) as imei,
         | first(imsi) as imsi,
         | starttime,
         | lac,
         | ci,
         | first(lon) as lon,
         | first(lat) as lat,
         | '$province' as source_province,
         | first(province) as province,
         | '$daytime' as daytime
         |from
         | $result_table_name
         |group by
         | msisdn, starttime, lac, ci
      """.stripMargin)

    r_df.write.mode(SaveMode.Append).partitionBy("province", "daytime")
      .parquet("hdfs://master1:9000/sunyj/out/people_loc")

    spark.stop()
  }

}