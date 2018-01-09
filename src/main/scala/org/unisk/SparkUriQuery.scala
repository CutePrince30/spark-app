package org.unisk

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.unisk.udf.{Idfa, UriPrefix}
import scala.util.Try

import scala.collection.mutable.ListBuffer

/**
  * uri匹配任务
  *
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object SparkUriQuery {

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

    import spark.sql
    import spark.implicits._

    /**
      * 所有的url业务数据 uri business
      */
    val uris_df = spark.sql("select uri, business from xdr.business_uris")

    val uri_bc = spark.sparkContext.broadcast(uris_df.rdd.map(row => {
      val uri_sep = row(0).toString.split("\\.")
      (uri_sep(0) + "." + uri_sep(1), Array(row(0).toString, row(1).toString))
    }).groupByKey().collectAsMap())

    def uriFilter(iter: Iterator[Row]): Iterator[(String, String, String, String, String, String, String, String, String, String)] = {
      val uri_map = uri_bc.value
      var list = ListBuffer[(String, String, String, String, String, String, String, String, String, String)]()
      iter.foreach(row => {
        Try {
          var uriSuffix = filter_uri(row.getString(4)) // www.baidu.com/music/....
          var key = getUriKey(uriSuffix) // www.baidu
          if (!uri_map.contains(key)) {
            uriSuffix = filter_uri(row.getString(6))
            key = getUriKey(uriSuffix)
          }
          if (!key.isEmpty && uri_map.contains(key)) {
            uri_map.get(key).foreach(iter => {
              iter.foreach(arr => {
                if (uriSuffix.startsWith(arr(0))) {
                  list append ((key, row.getString(0), row.getString(1), row.getString(2), row.getString(3),
                    row.getString(4), row.getString(5), row.getString(6), row.getString(7), arr(1)))
                }
              })
            })

//            uri_map.get(key).head.foreach(arr => {
//              if (uriSuffix.startsWith(arr(0))) {
//                list append ((key, row.getString(0), row.getString(1), row.getString(2), row.getString(3),
//                  row.getString(4), row.getString(5), row.getString(6), row.getString(7), arr(1)))
//              }
//            })
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
         | uri,
         | user_agent as ua,
         | refer,
         | starttime
         |from
         | xdr.http
         |where
         | province = '$province' and daytime = '$daytime' and msisdn != '' and uri != ''
      """.stripMargin)

    var r_df = http_df.mapPartitions(uriFilter).toDF("uri_key", "msisdn", "imsi", "imei", "idfa", "uri", "ua", "refer", "starttime", "business")

    val result_table_name = s"uri_${province}_${daytime}_result"
    r_df.createOrReplaceTempView(result_table_name)

    r_df = spark.sql(
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
        | business,
        | '$daytime' as daytime,
        | '$province' as province
        |from
        | $result_table_name
      """.stripMargin)

    r_df.write.mode(SaveMode.Append).partitionBy("business", "daytime", "province")
      .parquet("hdfs://master1:9000/sunyj/out/uris")

    spark.stop()
  }

}
