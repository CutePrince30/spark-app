package org.unisk

import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

/**
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object SparkLocData2EsApp {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: BeijingTaxi <daytime>")
      System.exit(1)
    }

    val province = args(0)
    val daytime = args(1)

    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName("SparkLocData2EsApp_" + province + "_" + daytime)
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("spark.files.ignoreCorruptFiles", value = true)
      .config("spark.sql.autoBroadcastJoinThreshold", 524288000)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import org.elasticsearch.spark._

    val writeConfig = Map(
      "es.nodes" -> "10.245.5.31,10.245.5.32,10.245.5.33",
      "es.index.auto.create" -> "false",
      "es.batch.size.entries" -> "30000",
      "es.batch.write.retry.count" -> "20",
      "es.batch.write.retry.wait" -> "30s"
    )

    val locDF = spark.read.parquet(s"/sunyj/out/people_loc/$daytime/province=$province/*.parquet")
    locDF.createOrReplaceTempView(s"${province}_loc")

    val resultDF = spark.sql(
      s"""
         |select
         | msisdn as id,
         | unix_timestamp(starttime, 'yyyyMMddHHmmss') as ts,
         | lon,
         | lat
         |from ${province}_loc
         |order by id, ts asc
      """.stripMargin)

    def filter(id: Long, postions: Iterable[(Long, Int, Int)]): (Long, ListBuffer[(Long, Int, Int)]) = {
      // ts,  lon, lat, lac, ci
      val r_positions = ListBuffer[(Long, Int, Int)]()
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

      (id, r_positions)
    }

    val locRDD = resultDF.map {
      case Row(id: Long, ts: Long, lon: Double, lat: Double) =>
        id -> (ts, (lon * 1000).toInt, (lat * 1000).toInt)
    }.rdd.groupByKey().mapPartitions(iter => {
      val rlist = ListBuffer[Map[String, Any]]()
      iter.foreach(row => {
        val (id, r_positions) = filter(row._1, row._2)
        for (r_position <- r_positions) {
          rlist append
            Map("msisdn" -> id,
                "starttime" -> DateFormatUtils.format(r_position._1, "yyyy-MM-dd HH:mm:ss"),
                "lon" -> r_position._2,
                "lat" -> r_position._3)
        }
      })
      rlist.iterator
    })

    EsSpark.saveToEs(locRDD, "loc/docs", writeConfig)

    spark.stop()
  }

}
