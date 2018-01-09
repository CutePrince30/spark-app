package org.unisk

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 鲜果广告任务
  *
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object SparkXianGuoQuery1 {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: SparkXianGuoQuery <province> <daytime>")
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


    import spark.sql

    val xianguo_common_df = spark.read.parquet(s"/sunyj/out/uris/business=xianguo/daytime=$daytime/*.parquet")
    xianguo_common_df.createOrReplaceTempView("xianguo_common")

    val baidu_xianguo_df = spark.read.parquet(s"/sunyj/out/uris/business=baidu/daytime=$daytime/*.parquet")
    baidu_xianguo_df.createOrReplaceTempView("baidu_xianguo")
    val baidu_keywords = "%E7%BD%91%E8%B4%B7%E6%9C%89%E5%93%AA%E4%BA%9B%E5%AE%B9%E6%98%93%E5%80%9F%E9%92%B1|%E7%BD%91%E7%BB%9C%E8%B4%B7%E6%AC%BE%E6%80%8E%E4%B9%88%E6%A0%B7|%E7%BD%91%E4%B8%8A%E8%B4%B7%E6%AC%BE%E9%9C%80%E8%A6%81%E4%BB%80%E4%B9%88%E5%90%97|%E4%B8%8D%E9%9C%80%E8%A6%81%E5%BE%81%E4%BF%A1%E7%9A%84%E7%BD%91%E7%BB%9C%E8%B4%B7%E6%AC%BE|%E7%BD%91%E7%BB%9C%E8%B4%B7%E6%AC%BE%E6%98%AF%E7%9C%9F%E7%9A%84%E5%90%97|2017%E6%9C%80%E5%AE%B9%E6%98%93%E4%B8%8B%E6%AC%BE%E7%9A%84%E7%BD%91%E8%B4%B7|%E5%85%8D%E6%81%AF%E8%B4%B7%E6%AC%BE|%E9%9D%A0%E8%B0%B1%E8%B4%A7%E6%AC%BE|%E6%A0%A1%E5%9B%AD%E8%B4%A7%E6%AC%BE|%E8%B4%B7%E6%AC%BE%E5%B9%B3%E5%8F%B0|%E5%BF%AB%E9%80%9F%E6%94%BE%E8%B4%B7|%E6%97%A0%E6%8A%B5%E6%8A%BC%E8%B4%B7%E6%AC%BE|%0D%0A%E5%8F%AF%E5%88%86%E6%9C%9F%E8%BF%98%E6%AC%BE%E6%9C%BA%E6%9E%84|%E4%B8%87%E7%A7%91%E6%B5%B7%E4%B8%8A%E4%BC%A0%E5%A5%87%E6%98%86%E5%B1%B1|%E6%98%86%E5%B1%B1%E4%B8%87%E7%A7%91%E6%B5%B7%E4%B8%8A%E4%BC%A0%E5%A5%87"

    val xianguo_out = sql(
      s"""
         |select
         | id,
         | first(imsi),
         | first(imei),
         | collect_set(idfa)[0],
         | first(uri),
         | first(refer),
         | first(ua),
         | first(starttime),
         | first(province)
         |from
         |(
         |	select
         |	 md5(concat('xianguo', msisdn)) as id,
         |	 imsi,
         |	 imei,
         |	 idfa,
         |	 uri,
         |	 ua,
         |	 refer,
         |	 starttime,
         |	 province
         |	from
         |	 xianguo_common
         |
         |	union all
         |
         |  select
         |	 md5(concat('xianguo', msisdn)) as id,
         |	 imsi,
         |	 imei,
         |	 idfa,
         |	 uri,
         |	 ua,
         |	 refer,
         |	 starttime,
         |	 province
         |	from
         |	 baidu_xianguo
         |	where concat(uri, refer) regexp '($baidu_keywords)'
         |) tmp
         |group by id
         |
      """.stripMargin)

    xianguo_out.write.option("sep", "\t").mode(SaveMode.Append)
      .csv(s"hdfs://master1:9000/sunyj/out/xianguo/$daytime")

    spark.stop()
  }

}
