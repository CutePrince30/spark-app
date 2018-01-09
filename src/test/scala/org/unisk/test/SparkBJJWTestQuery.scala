package org.unisk.test

import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * 北京交委任务
  *
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object SparkBJJWTestQuery {

  def merge(iter: Iterator[Row]): Iterator[(String, String, String, String, String)] = {
    val r_iter = iter.toSet.iterator
    var res = List[(String, String, String, String, String)]()
    while (r_iter.hasNext) {
      val cur = r_iter.next
      val cur_cols = cur.toSeq.toList
      res :+= (cur_cols(0).toString, cur_cols(1).toString,
        cur_cols(2).toString, cur_cols(3).toString, cur_cols(4).toString)
    }
    res.iterator
  }

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
//      .config("hive.exec.compress.output", value = true)
//      .config("mapred.output.compress", value = true)
//      .config("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
//      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

    val http_df = spark.sql(
      "select md5(concat(imsi, imei, msisdn)) as id, " +
        "from_unixtime(unix_timestamp(starttime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHHmmss') as starttime, " +
        "concat(lac_tac, '_', ci) as pid, " +
        "case length(msisdn) when 13 then substr(msisdn, 3, 7) else substr(msisdn, 1, 7) end as segment, " +
        "province " +
      "from xdr.http " +
      "where " +
      "province = '" + province + "' and daytime = '" + daytime + "' and msisdn regexp '^(86)?1[^(0|1|2)][0-9]{9}$'")

//    http_df.rdd.mapPartitions(merge).toDF("id", "starttime", "pid", "segment", "province")
//      .createOrReplaceTempView("http")

    http_df.createOrReplaceTempView("http")

    /**
      * id, starttime, lac, ci, lon, lat, province1, province2, city, province, daytime
      */
    val df = sql(
      "select a.id as id, " +
        "a.starttime as starttime, " +
        "split(a.pid, '_')[0] as lac, " +
        "split(a.pid, '_')[1] as ci, " +
        "a.lon as lon, " +
        "a.lat as lat, " +
        "a.province as province1, " +
        "if(b.province is NULL, '', b.province) as province2, " +
        "if(b.city is NULL, '', b.city) as city, " +
        "a.cell_province as province, " +
        "'" + daytime + "' as daytime " +
      "from " +
        "(select xh.id as id, " +
          "xh.starttime as starttime, " +
          "xh.pid as pid, " +
          "xl.lon as lon, " +
          "xl.lat as lat, " +
          "xh.province as province, " +
          "xh.segment as segment, " +
          "xl.province as cell_province " +
        "from http as xh " +
        "left join " +
          "(select " +
            "concat(lac, '_', ci) as pid, " +
            "lon, lat, province " +
          "from xdr.location " +
          "where " +
            "province in ('hubei', 'beijing', 'tianjin', 'hebei', 'guangdong', 'jiangsu', 'zhejiang', 'shanghai') and city = 'all') as xl " +
        "on xh.pid = xl.pid where xl.lon is not null and xl.lat is not null) as a " +
      "left join " +
        "xdr.phone_num_info b " +
      "on a.segment = b.segment"
    )

    df.write.mode(SaveMode.Append).option("compression", "gzip")
      .partitionBy("province", "daytime").csv("hdfs://master1:9000/sunyj/out/active_loc")

    spark.stop()
  }

}
