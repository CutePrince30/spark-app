package org.unisk.gdjwloc

import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

/**
  * 广东交委位置任务
  *
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object SparkGdjwLocQuery {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: SparkGdjwLocQuery <daytime>")
      System.exit(1)
    }

    val province = args(0)
    val daytime = args(1)

    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName("gdjw_loc_" + province + "_" + daytime)
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("spark.files.ignoreCorruptFiles", value = true)
      .config("spark.sql.autoBroadcastJoinThreshold", 524288000)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val httpDF = spark.read.parquet(s"/sunyj/out/people_loc/province=$province/daytime=$daytime/*.parquet")
    httpDF.createOrReplaceTempView(s"${province}_loc")

    val resultDF = spark.sql(
      s"""
        |select
        | md5(concat(imsi, imei, msisdn)) as id,
        | unix_timestamp(starttime, 'yyyyMMddHHmmss') as ts,
        | lac,
        | ci,
        | lon,
        | lat
        |from ${province}_loc
        |order by id, ts asc
      """.stripMargin)

    def filter(id: String, postions: Iterable[(Long, Int, Int, Int, Int)]): (String, String) = {
      val r_positions = ListBuffer[(Long, Int, Int, Int, Int)]()
      val o_positions = postions.toList

      var i = 0
      breakable {
        while (i < o_positions.length) {
          val f1 = i + 1
          var f2 = i + 2

          if (f1 == o_positions.length) {
            r_positions.append(o_positions(i))
            break
          }

          if (f2 == o_positions.length) {
            f2 = f1
          }

          if (o_positions(i)._2 == o_positions(f1)._2 && o_positions(i)._3 == o_positions(f1)._3) {
            i = f1
          }
          else {
            if (o_positions(i)._2 == o_positions(f2)._2 && o_positions(i)._3 == o_positions(f2)._3
              && o_positions(f2)._1 - o_positions(i)._1 < 30 * 60) {
              i = f2
            }
            else {
              r_positions.append(o_positions(i))
              i = f1
            }
          }
        }
      }

      (id, r_positions.mkString("[", ",", "]"))
    }


    resultDF.map {
      case Row(id: String, ts: Long, lac: String, ci: String, lon: String, lat: String) =>
        id -> (ts, (lon.toFloat * 1000).toInt, (lat.toFloat * 1000).toInt, lac.toInt, ci.toInt)
    }.rdd.groupByKey().mapPartitions(iter => {
      val rlist = ListBuffer[(String, String)]()
      iter.foreach(row => {
        rlist.append(filter(row._1, row._2))
      })
      rlist.iterator
    }).saveAsTextFile(s"hdfs://master1:9000/sunyj/out/xinling/$province/$daytime",
      classOf[org.apache.hadoop.io.compress.GzipCodec])

    spark.stop()
  }

}
