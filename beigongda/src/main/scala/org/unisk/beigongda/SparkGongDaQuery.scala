package org.unisk.beigongda

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 工大任务
  *
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object SparkGongDaQuery {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: SparkGongDaQuery <province> <daytime>")
      System.exit(1)
    }

    val province = args(0)
    val daytime = args(1)

    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName("gongda_" + province + "_" + daytime)
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("spark.files.ignoreCorruptFiles", value = true)
      .config("spark.sql.autoBroadcastJoinThreshold", 524288000)
      .config("hive.groupby.orderby.position.alias", value = true)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

    val peopleLocDF = spark.read.parquet("hdfs://master1:9000/sunyj/out/people_loc/province="
      + province + "/daytime=" + daytime + "/*")

    peopleLocDF.createOrReplaceTempView("people_loc")

    var province_code = ""
    province match {
      case "beijing" => province_code = "011"
      case "zhejiang" => province_code = "036"
      case "jiangsu" => province_code = "034"
      case "shanghai" => province_code = "031"
    }

    if (province_code == "") {
      System.err.println("Error: province code is empty")
      System.exit(1)
    }

    val df = sql(
      s"""
        |select
        |	a.id as id,
        | a.starttime as starttime,
        | a.lac as lac,
        | a.ci as ci,
        | a.lon as lon,
        | a.lat as lat,
        | b.age as age
        |from
        |	(select
        |			md5(concat(imsi, imei, msisdn)) as id,
        |			case length(msisdn) when 13 then substr(msisdn, 3, 11) else msisdn end as msisdn,
        |			starttime,
        |			lac,
        |			ci,
        |			lon,
        |			lat
        |		from
        |			people_loc
        |		) a
        |left join
        |	(select msisdn, age from xdr.people_base where province_code = '$province_code') b
        |on a.msisdn = b.msisdn
      """.stripMargin
    )

    df.write.mode(SaveMode.Append).option("compression", "gzip")
      .csv("hdfs://master1:9000/sunyj/out/zhuyun/loc/gongda/" + province + "/" + daytime)

    spark.stop()
  }

}
