package org.unisk.business

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.unisk.business.udf.{Idfa, UriPrefix}

import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
  * uri匹配任务
  *
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object SparkUniskBusinessQuery {

  val idfa = new Idfa
  val uriPrefix = new UriPrefix

  val filter_idfa: String => String = (log: String) => {
    idfa.get_idfa(log)
  }

  val filter_uri: String => String = (log: String) => {
    uriPrefix.get_uri(log)
  }

  def getUriKey(uri: String): String = {
    if (!uri.isEmpty && uri.contains(".")) {
      val uri_sep = uri.split("\\.")
      uri_sep(0) + "." + uri_sep(1)
    } else {
      uri
    }
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: SparkUriQuery <province> <daytime>")
      System.exit(1)
    }

    val province = args(0)
    val daytime = args(1)

    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName("uri_" + province + "_" + daytime)
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("spark.files.ignoreCorruptFiles", value = true)
      .config("spark.sql.autoBroadcastJoinThreshold", 524288000)
      .config("hive.groupby.orderby.position.alias", value = true)
      .enableHiveSupport()
      .getOrCreate()

    spark.udf.register("idfa", filter_idfa)

    import spark.implicits._

    /**
      * URL信息的基础表，需要跑的全国的或者本省的url业务数据 uri business
      */
    val uris_df = spark.sql(s"select uri, business from xdr.business_uris where province in ('all', '$province')")

    val uri_bc = spark.sparkContext.broadcast(uris_df.rdd.map(row => {
      val uri_sep = row(0).toString.split("\\.")
      (uri_sep(0) + "." + uri_sep(1), Array(row(0).toString, row(1).toString))
    }).groupByKey().collectAsMap())

    /**
      * 位置信息的基础表
      */
    val loc_df = spark.sql(
      """
        |select
        | concat_ws('_', lac, ci) as pid,
        | concat_ws('_', lon, lat, province) as ploc
        |from
        | xdr.location
        |where
        | province in ('hubei', 'beijing', 'tianjin', 'hebei', 'guangdong', 'jiangsu', 'zhejiang', 'shanghai', 'jiangxi', 'gansu') and city = 'all'
      """.stripMargin)

    val loc_info = loc_df.rdd.map(record => record(0) -> record(1)).collectAsMap()

    val loc_bc = spark.sparkContext.broadcast(loc_info)

    /**
      * 位置信息的过滤器
      * @param iter
      * @return
      */
    def locFilter(iter: Iterator[Row]): Iterator[(String, String, String, String, String, String, String, String, String)] = {
      val loc_map = loc_bc.value
      var set = scala.collection.mutable.Set[(String, String, String, String, String, String, String, String, String)]()
      iter.foreach(row => {
        val pid = row.getString(9) // pid
        if (loc_map.contains(pid)) {
          val loc_value = loc_map.get(pid).mkString.split("_")
          Try {
            set add ((row.getString(0), row.getString(1), row.getString(2), row.getString(8),
              pid.split("_")(0), pid.split("_")(1),
              loc_value(0), loc_value(1), loc_value(2)))
          }
        }
      })
      set.iterator
    }

    /**
      * 网址信息的过滤器
      * @param iter
      * @return
      */
    def uriFilter(iter: Iterator[Row]): Iterator[(String, String, String, String, String, String, String, String, String, String, String)] = {
      val uri_map = uri_bc.value
      var list = ListBuffer[(String, String, String, String, String, String, String, String, String, String, String)]()
      iter.foreach(row => {
        Try {
          var uri = row.getString(5)
          if (uri.startsWith("/")) {
            uri = row.getString(4).+(uri) // host + uri
          }
          var uriSuffix = filter_uri(uri) // uri: www.baidu.com/music/....
          var key = getUriKey(uriSuffix) // www.baidu
          if (!uri_map.contains(key)) {
            uriSuffix = filter_uri(row.getString(7)) // refer
            key = getUriKey(uriSuffix)
          }
          if (!key.isEmpty && uri_map.contains(key)) {
            uri_map.get(key).foreach(iter => {
              iter.foreach(arr => {
                var isAppend = false
                val parts_str = arr(0) // dealer.m.autohome.com.cn/dealer/|-2863.html
                if (parts_str.contains("|")) {
                  val parts_regexp = ("^" + parts_str.replaceAll("\\|", ".*")).r
                  if (parts_regexp.findFirstIn(uriSuffix).nonEmpty) {
                    isAppend = true
                  }
                }
                else {
                  if (uriSuffix.startsWith(parts_str)) {
                    isAppend = true
                  }
                }
                if (isAppend) {
                  list append ((key, row.getString(0), row.getString(1), row.getString(2), row.getString(3),
                    uri, row.getString(6), row.getString(7), row.getString(8), arr(1), arr(0)))
                }
              })
            })
          }
        }
      })
      list.iterator
    }

    val http_df = spark.sql(
      s"""
         |select
         | msisdn,
         | imsi,
         | imei,
         | idfa(uri) as idfa,
         | host,
         | uri,
         | user_agent as ua,
         | refer,
         | from_unixtime(unix_timestamp(starttime, 'yyyy-MM-dd HH:mm:ss'), 'yyyyMMddHHmmss') as starttime,
         | concat_ws('_', lac_tac, ci) as pid
         |from
         | xdr.http
         |where
         | province = '$province' and daytime = '$daytime' and msisdn != '' and uri != ''
      """.stripMargin).persist(StorageLevel.MEMORY_AND_DISK_SER)

    /**
      * 处理url逻辑
      */
    var uri_out_df = http_df.mapPartitions(uriFilter).
      toDF("uri_key", "msisdn", "imsi", "imei", "idfa", "uri", "ua", "refer", "starttime", "business", "keyword")

    val uri_out_df_table_name = s"uri_${province}_${daytime}_result"
    uri_out_df.createOrReplaceTempView(uri_out_df_table_name)

    uri_out_df = spark.sql(
      s"""
        |select
        | msisdn,
        | imsi,
        | imei,
        | idfa,
        | uri,
        | ua,
        | refer,
        | starttime,
        | keyword,
        | business,
        | '$daytime' as daytime,
        | '$province' as province
        |from
        | $uri_out_df_table_name
      """.stripMargin)

    /**
      * 输出url结果
      */
    uri_out_df.write.mode(SaveMode.Append).partitionBy("business", "daytime")
      .parquet("hdfs://master1:9000/sunyj/out/uris")




    /**
      * 处理位置信息逻辑
      */
    var loc_out_df = http_df.mapPartitions(locFilter).toDF("msisdn", "imei", "imsi", "starttime",
      "lac", "ci", "lon", "lat", "province")

    val loc_out_df_table_name = s"people_loc_${province}_${daytime}_result"
    loc_out_df.createOrReplaceTempView(loc_out_df_table_name)

    loc_out_df = spark.sql(
      s"""
         |select
         | msisdn,
         | imei,
         | imsi,
         | starttime,
         | lac,
         | ci,
         | lon,
         | lat,
         | '$province' as source_province,
         | province,
         | '$daytime' as daytime
         |from
         | $loc_out_df_table_name
      """.stripMargin)

    /**
      * 输出位置结果
      */
    loc_out_df.write.mode(SaveMode.Append).partitionBy("province", "daytime")
      .parquet("hdfs://master1:9000/sunyj/out/people_loc")


    spark.stop()
  }

}
