package org.unisk

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.unisk.udf.Idfa

/**
  * 北京四码基础库任务
  *
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object SparkIIIMQuery {

  val idfa = new Idfa

  val filter_idfa: String => String = (log: String) => {
    idfa.get_idfa(log)
  }

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

    spark.udf.register("idfa", filter_idfa)

    import spark.sql

    val df = sql(
      s"""
        |select
        |	msisdn,
        |	imei,
        |	imsi,
        |	idfa(uri) as idfa
        |from
        |	xdr.http
        |where
        |	province = '$province' and
        | daytime = '$daytime' and
        | msisdn regexp '^(86)?1[^(0|1|2)][0-9]{9}$$' and
        | imei != '' and imsi != ''
        |group by 1,2,3,4
      """.stripMargin)

    df.write.mode(SaveMode.Append).parquet("hdfs://master1:9000/sunyj/out/iiim/" + daytime + "/" + province)

    spark.stop()
  }

}
