package org.unisk.people.test

import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
  * 位置任务
  *
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object SparkPeopleLocationTest {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: SparkPeopleLocationTest <province> <daytime>")
      System.exit(1)
    }

    val province = args(0)
    val daytime = args(1)

    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName(s"people_loc_test_${province}_$daytime")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("spark.files.ignoreCorruptFiles", value = true)
      .config("spark.sql.autoBroadcastJoinThreshold", 524288000)
      .config("hive.groupby.orderby.position.alias", value = true)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

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
      val set = scala.collection.mutable.Set[(String, String, String, String, String, String, String, String, String)]()
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

    val r_df = http_df.mapPartitions(merge).toDF("msisdn", "imei", "imsi", "starttime",
      "lac", "ci", "lon", "lat", "province")
      .map {
        row =>
          (row.getString(0), row.getString(3), row.getString(4), row.getString(5)) ->
            (row.getString(1), row.getString(2), row.getString(6), row.getString(7), row.getString(8))
      }
      .rdd
      .reduceByKey((x, y) => {
        (x._1, x._2, x._3, x._4, x._5)
      })
      .mapPartitions(rows => {
        val list = ListBuffer[(String, String, String, String,
          String, String, String, String, String, String, String)]()
        rows.foreach(x => {
          list.append((x._1._1, x._1._2, x._1._3, x._1._4, x._2._1,
            x._2._2, x._2._3, x._2._4, x._2._5, province, daytime))
        })
        list.iterator
      })
      .toDF("msisdn", "imei", "imsi", "starttime", "lac", "ci", "lon",
        "lat", "province", "source_province", "daytime")

//      .map(x => (x._1._1, x._1._2, x._1._3, x._1._4, x._2._1,
//        x._2._2, x._2._3, x._2._4, x._2._5, province, daytime))

    r_df.write.mode(SaveMode.Append).partitionBy("province", "daytime")
      .parquet("hdfs://master1:9000/sunyj/test/people_loc")

    spark.stop()
  }

}