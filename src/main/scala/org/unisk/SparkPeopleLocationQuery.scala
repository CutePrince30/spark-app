package org.unisk

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 北京交委任务
  *
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object SparkPeopleLocationQuery {

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

    val df = sql(
      s"""
         |select
         |	xh.msisdn as msisdn,
         |	xh.imei as imei,
         |	xh.imsi as imsi,
         |	xh.starttime as starttime,
         |	split(xh.pid, '_')[0] as lac,
         |	split(xh.pid, '_')[1] as ci,
         |	xl.lon as lon,
         |	xl.lat as lat,
         |	xh.province as source_province,
         |	xl.province as province,
         |  '$daytime' as daytime
         |from
         |	(select
         |		collect_set(imsi)[0] as imsi,
         |		collect_set(imei)[0] as imei,
         |		msisdn,
         |		from_unixtime(unix_timestamp(starttime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHHmmss') as starttime,
         |		concat(lac_tac, '_', ci) as pid,
         |		collect_set(province)[0] as province
         |	from
         |		xdr.http
         |	where province = '$province' and daytime = '$daytime' and msisdn regexp '^(86)?1[^(0|1|2)][0-9]{9}$$' and imei != ''
         |	group by 3,4,5) as xh
         |left join
         |	(select concat(lac, '_', ci) as pid, lon, lat, province
         |	from
         |	  xdr.location
         |	where
         |	  province in ('hubei', 'beijing', 'tianjin', 'hebei', 'guangdong', 'jiangsu', 'zhejiang', 'shanghai') and city = 'all'
         |	) as xl
         |on
         |	xh.pid = xl.pid
         |where
         |	xl.lon is not null and xl.lat is not null
      """.stripMargin
    )

    df.write.mode(SaveMode.Append).partitionBy("province", "daytime")
      .parquet("hdfs://master1:9000/sunyj/out/people_loc")

    spark.stop()
  }

}
