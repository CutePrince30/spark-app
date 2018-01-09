package org.unisk.bjjw

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 北京交委任务
  *
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object SparkBJJWQuery {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: SparkBJJWQuery <province> <daytime>")
      System.exit(1)
    }

    val province = args(0)
    val daytime = args(1)

    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName("bjjw_" + province + "_" + daytime)
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

    val df = sql(
      s"""
        |select
        |	a.id as id,
        |	a.starttime as starttime,
        |	a.lac as lac,
        |	a.ci as ci,
        |	a.lon as lon,
        |	a.lat as lat,
        |	a.source_province as source_province,
        |	if(b.province is NULL, '', b.province) as phone_province,
        |	if(b.city is NULL, '', b.city) as phone_city
        |from
        |	(select
        |		md5(concat(imsi, imei, msisdn)) as id,
        |		starttime,
        |		lac,
        |		ci,
        |		lon,
        |		lat,
        |		source_province,
        |		case length(msisdn) when 13 then substr(msisdn, 3, 7) else substr(msisdn, 1, 7) end as segment
        |	from
        |		people_loc
        |	) a
        |left join
        |	xdr.phone_num_info b
        |on a.segment = b.segment
      """.stripMargin)

    df.write.mode(SaveMode.Append).option("compression", "gzip")
      .csv("hdfs://master1:9000/sunyj/out/zhuyun/loc/bjjw/" + province + "/" + daytime)

    spark.stop()
  }

}
