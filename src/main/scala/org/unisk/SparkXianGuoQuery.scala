package org.unisk

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.unisk.udf.Idfa

/**
  * 鲜果广告任务
  *
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object SparkXianGuoQuery {

  val idfa = new Idfa

  val filter_idfa: String => String = (log: String) => {
    idfa.get_idfa(log)
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: SparkXIANGUOQuery <province> <daytime>")
      System.exit(1)
    }

    val province = args(0)
    val daytime = args(1)

    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName("xianguo_" + province + "_" + daytime)
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("spark.files.ignoreCorruptFiles", value = true)
      .config("spark.sql.autoBroadcastJoinThreshold", 524288000)
      .config("hive.groupby.orderby.position.alias", value = true)
      .enableHiveSupport()
      .getOrCreate()

    spark.udf.register("idfa", filter_idfa)

    import spark.sql

    sql(
      """
        |CACHE TABLE xianguo_uri As
        |select
        | uri
        |from
        | xdr.business_uris
        |where
        | business = 'xianguo' and version='1'
      """.stripMargin)

    val df = sql(
      s"""
         |select
         |	a.msisdn as msisdn,
         |	collect_set(a.imsi)[0] as imsi,
         |	collect_set(a.imei)[0] as imei,
         |	collect_set(idfa(a.uri))[0] as idfa,
         |	collect_set(a.uri)[0] as uri,
         |	collect_set(a.user_agent)[0] as ua,
         |	collect_set(a.refer)[0] as refer,
         |	collect_set(a.starttime)[0] as starttime,
         |	collect_set(a.province)[0] as province
         |from
         |	xdr.http a
         |join
         |	xianguo_uri b
         |on
         |	a.uri = b.uri
         |where
         |	a.province='$province' and a.daytime='$daytime' and a.msisdn != '' and a.user_agent not like '%windows%'
         |group by a.msisdn
      """.stripMargin)

    val result_table_name = s"xianguo_${province}_${daytime}_result"
    df.createOrReplaceTempView(result_table_name)

    spark.sql(s"""select concat_ws("\t", md5(msisdn), imsi, imei, idfa, uri, ua, refer, starttime, province) as r from $result_table_name""")
      .write.mode(SaveMode.Append)
      .text(s"hdfs://master1:9000/sunyj/out/xianguo/$daytime")

    spark.stop()
  }

}
