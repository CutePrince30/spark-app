package org.unisk

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 北京交委任务
  *
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object SparkPeopleLocationQuery1 {

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
      .appName("people_loc_" + province + "_" + daytime)
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("spark.files.ignoreCorruptFiles", value = true)
      .config("spark.sql.autoBroadcastJoinThreshold", 524288000)
      .config("hive.groupby.orderby.position.alias", value = true)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

    sql(
      """
        |CACHE TABLE people_loc_locations As
        |select concat(lac, '_', ci) as pid, lon, lat, province
        |	from
        |	  xdr.location
        |	where
        |	  province in ('hubei', 'beijing', 'tianjin', 'hebei', 'guangdong', 'jiangsu', 'zhejiang', 'shanghai') and city = 'all'
      """.stripMargin)

    val df = sql(
      s"""
         |select
         |	msisdn,
         |	imei,
         |	imsi,
         |	starttime,
         |	lac,
         |	ci,
         |	lon,
         |	lat,
         |	source_province,
         |	province,
         |	'$daytime' as daytime
         |from
         |	(select
         |		a.msisdn as msisdn,
         |		from_unixtime(unix_timestamp(a.starttime, 'yyyy-MM-dd HH:mm:ss'), 'yyyyMMddHHmmss') as starttime,
         |		a.lac_tac as lac,
         |    a.ci as ci,
         |		collect_set(a.imsi)[0] as imsi,
         |		collect_set(a.imei)[0] as imei,
         |		collect_set(b.lon)[0] as lon,
         |		collect_set(b.lat)[0] as lat,
         |		collect_set(a.province)[0] as source_province,
         |		collect_set(b.province)[0] as province
         |	from
         |		xdr.http a
         |	join
         |		people_loc_locations b
         |	on
         |		concat(a.lac_tac, '_', a.ci) = b.pid
         |	where
         |		a.province = '$province' and a.daytime = '$daytime' and a.msisdn regexp '^(86)?1[^(0|1|2)][0-9]{9}$$' and a.imei != ''
         |	group by 1,2,3,4)
      """.stripMargin
    )

    df.write.mode(SaveMode.Append).partitionBy("province", "daytime")
      .parquet("hdfs://master1:9000/sunyj/test/people_loc")

    spark.stop()
  }

}
