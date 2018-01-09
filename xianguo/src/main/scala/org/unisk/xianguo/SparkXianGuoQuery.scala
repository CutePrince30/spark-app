package org.unisk.xianguo

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 鲜果广告任务
  *
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object SparkXianGuoQuery {

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
    val baidu_keywords = "%E6%89%98%E7%A6%8F|%E9%9B%85%E6%80%9D|SAT|GRE|%E5%87%BA%E5%9B%BD|%E7%95%99%E5%AD%A6|%E9%9B%85%E6%80%9D%E5%8F%A3%E8%AF%AD|%E6%89%98%E7%A6%8F%E5%8F%A3%E8%AF%AD|%E9%9B%85%E6%80%9D%E5%86%99%E4%BD%9C|%E6%89%98%E7%A6%8F%E5%86%99%E4%BD%9C|%E6%89%98%E7%A6%8F%E6%8A%A5%E5%90%8D|%E9%9B%85%E6%80%9D%E6%8A%A5%E5%90%8D|%E6%89%98%E7%A6%8F%E5%AE%98%E7%BD%91|%E9%9B%85%E6%80%9D%E5%AE%98%E7%BD%91|%E9%9B%85%E6%80%9D%E8%80%83%E8%AF%95%E6%97%B6%E9%97%B4|%E6%89%98%E7%A6%8F%E8%80%83%E8%AF%95%E6%97%B6%E9%97%B4|%E7%BE%8E%E5%9B%BD%E7%95%99%E5%AD%A6|%E8%8B%B1%E5%9B%BD%E7%95%99%E5%AD%A6|%E6%BE%B3%E5%A4%A7%E5%88%A9%E4%BA%9A%E7%95%99%E5%AD%A6|%E5%8A%A0%E6%8B%BF%E5%A4%A7%E7%95%99%E5%AD%A6|%E6%BE%B3%E6%B4%B2%E7%95%99%E5%AD%A6|%E9%9B%85%E6%80%9D%E5%90%AC%E5%8A%9B|%E6%89%98%E7%A6%8F%E5%90%AC%E5%8A%9B|%E9%9B%85%E6%80%9D%E9%98%85%E8%AF%BB|%E6%89%98%E7%A6%8F%E9%98%85%E8%AF%BB|%E6%9C%AC%E7%A7%91%E5%87%BA%E5%9B%BD|%E9%AB%98%E4%B8%AD%E5%87%BA%E5%9B%BD|%E7%A0%94%E7%A9%B6%E7%94%9F%E5%87%BA%E5%9B%BD|%E7%BE%8E%E5%9B%BD%E7%95%99%E5%AD%A6%E6%9D%A1%E4%BB%B6|%E8%89%BA%E6%9C%AF%E7%94%9F%E7%95%99%E5%AD%A6|%E7%95%99%E5%AD%A6%E4%B8%80%E5%B9%B4%E8%B4%B9%E7%94%A8|%E7%BE%8E%E5%9B%BD%E7%95%99%E5%AD%A6%E8%B4%B9%E7%94%A8|%E8%AF%AD%E8%A8%80%E8%80%83%E8%AF%95|%E6%A4%8D%E5%8F%91|%E6%A4%8D%E5%8F%91%E5%A4%9A%E5%B0%91%E9%92%B1|%E6%A4%8D%E5%8F%91%E4%B8%80%E8%88%AC%E4%BB%B7%E6%A0%BC%E6%98%AF%E5%A4%9A%E5%B0%91|%E8%83%BD%E7%A7%8D%E5%A4%B4%E5%8F%91%E5%90%97|%E6%A4%8D%E5%8F%91%E5%A4%B4%E5%8F%91|%E6%A4%8D%E5%8F%91%E4%BB%B7%E6%A0%BC%E5%A4%9A%E5%B0%91%E9%92%B1|%E7%A7%8D%E6%A4%8D+%E5%A4%B4%E5%8F%91|%E6%A4%8D%E5%8F%91%E4%B8%80%E8%88%AC%E4%BB%B7%E6%A0%BC%E5%A4%9A%E5%B0%91|%E6%A4%8D%E5%8F%91%E9%9D%A0%E8%B0%B1%E5%90%97|%E6%A4%8D%E5%8F%91%E6%9C%89%E6%B2%A1%E6%9C%89%E5%89%AF%E4%BD%9C%E7%94%A8|%E5%A4%B4%E5%8F%91%E6%A4%8D%E5%8F%91%E8%A6%81%E5%A4%9A%E5%B0%91%E9%92%B1|%E6%A4%8D%E5%8F%91%E4%B8%80%E8%88%AC%E9%9C%80%E8%A6%81%E5%A4%9A%E5%B0%91%E9%92%B1|%E8%83%BD%E7%A7%8D%E6%A4%8D%E5%A4%B4%E5%8F%91%E5%90%97|%E5%93%AA%E5%AE%B6%E5%8C%BB%E9%99%A2%E5%8F%AF%E4%BB%A5%E6%A4%8D%E5%8F%91|%E6%A4%8D%E5%8F%91%E6%89%8B%E6%9C%AF%E4%BB%B7%E6%A0%BC%E6%98%AF%E5%A4%9A%E5%B0%91|%E6%A4%8D%E5%8F%91%E6%89%8B%E6%9C%AF%E5%A4%A7%E6%A6%82%E5%A4%9A%E5%B0%91%E9%92%B1|%E6%A4%8D%E5%8F%91%E5%A4%A7%E6%A6%82%E4%BB%B7%E6%A0%BC|%E6%A4%8D%E5%8F%91+%E9%9D%A0%E8%B0%B1%E5%90%97|%E6%A4%8D%E5%8F%91%E7%9A%84%E4%BB%B7%E6%A0%BC%E4%B8%80%E8%88%AC%E6%98%AF%E5%A4%9A%E5%B0%91|%E6%A4%8D%E5%8F%91%E4%B8%80%E8%88%AC%E8%A6%81%E5%A4%9A%E5%B0%91%E9%92%B1|%E5%A4%B4%E5%8F%91%E7%A7%8D%E6%A4%8D%E8%B4%B9%E7%94%A8|%E6%83%B3%E6%A4%8D%E5%8F%91|%E6%A4%8D%E5%8F%91%E8%A6%81%E5%A4%9A%E5%B0%91%E9%92%B1|%E6%A4%8D%E5%8F%91%E5%93%AA%E5%AE%B6%E5%BC%BA|%E6%A4%8D%E5%8F%91%E4%BB%B7%E6%A0%BC%E4%B8%80%E8%88%AC%E5%A4%9A%E5%B0%91%E9%92%B1|%E4%BC%A4%E7%96%A4%E6%A4%8D%E5%8F%91|%E8%A1%A5%E5%8F%91|%E6%A4%8D%E5%8F%91%E4%BB%B7%E6%A0%BC%E4%BB%B7%E6%A0%BC|%E6%A4%8D%E5%8F%91%E5%8A%A0%E5%AF%86|%E5%A4%B4%E5%8F%91+%E5%A4%B4%E5%8F%91%E7%A7%8D%E6%A4%8D|%E6%A4%8D%E5%8F%91%E5%A4%A7%E6%A6%82%E4%BB%B7%E6%A0%BC%E6%98%AF%E5%A4%9A%E5%B0%91|%E5%B8%82%E6%A4%8D%E5%8F%91%E5%8C%BB%E9%99%A2%E6%8E%92%E5%90%8D|%E6%A4%8D%E5%8F%91%E6%89%8B%E6%9C%AF%E8%B4%B5%E5%90%97|%E6%A4%8D%E5%8F%91%E6%89%8B%E6%9C%AF%E7%9A%84%E4%BB%B7%E6%A0%BC|%E6%A4%8D%E5%8F%91%E4%BB%B7%E6%A0%BC%E8%B4%B5%E5%90%97|%E6%AF%9B%E5%8F%91%E7%A7%8D%E6%A4%8D%E5%93%AA%E9%87%8C%E5%A5%BD|%E6%A4%8D%E5%8F%91%E6%94%B6%E8%B4%B9|%E6%A4%8D%E5%8F%91%E5%8C%BB%E9%99%A2|%E5%A4%A7%E6%A4%8D%E5%8F%91%E5%8C%BB%E9%99%A2|%E5%A4%B4%E5%8F%91%E6%A4%8D%E5%8F%91%E4%BB%B7%E6%A0%BC|%E6%A4%8D%E5%8F%91%E5%A4%9A%E5%B0%91%E4%BB%B7%E6%A0%BC|%E5%93%AA%E5%AE%B6%E5%8C%BB%E9%99%A2%E6%9C%89%E6%A4%8D%E5%8F%91|%E6%A4%8D%E5%8F%91+%E4%BB%B7%E9%92%B1|%E5%A4%B4%E5%8F%91%E7%A7%8D%E6%A4%8D%E6%94%B6%E8%B4%B9%E5%A4%9A%E5%B0%91|%E7%A7%8D%E6%A4%8D%E5%A4%B4%E5%8F%91%E4%BB%B7%E6%A0%BC|%E5%A4%B4%E5%8F%91%E7%A7%8D%E6%A4%8D%E6%9C%89%E7%94%A8%E5%90%97|%E6%A4%8D%E5%8F%91%E8%83%BD%E4%BF%9D%E6%8C%81%E5%A4%9A%E4%B9%85|%E7%A7%83%E9%A1%B6%E6%A4%8D%E5%8F%91%E9%9C%80%E8%A6%81%E5%A4%9A%E5%B0%91%E9%92%B1|%E5%A4%B4%E5%8F%91%E7%A7%8D%E6%A4%8D%E5%93%AA%E9%87%8C%E5%A5%BD|%E5%A4%B4%E5%8F%91%E7%A7%8D%E6%A4%8D%E5%90%8E|%E4%BA%BA%E5%B7%A5%E6%A4%8D%E5%8F%91|%E8%A1%A5%E5%A4%B4%E5%8F%91|%E5%A4%B4%E5%8F%91%E7%A7%8D%E6%A4%8D%E7%9A%84%E5%9C%B0%E6%96%B9|%E6%A4%8D%E5%8F%91+%E8%84%B1%E8%90%BD|%E6%A4%8D%E5%8F%91%E6%80%8E%E4%B9%88%E6%A0%B7|%E5%A4%B4%E5%8F%91%E7%A7%8D%E6%A4%8D%E5%93%AA%E5%AE%B6%E5%A5%BD|%E6%A4%8D%E5%A4%B4%E5%8F%91|%E5%8A%A0%E5%AF%86%E5%A4%B4%E5%8F%91|%E5%A4%B4%E5%8F%91+%E6%A4%8D%E5%8F%91|%E6%9C%89%E7%A7%8D%E5%A4%B4%E5%8F%91%E5%90%97|%E7%A7%8D+%E5%A4%B4%E5%8F%91|%E5%A4%B4%E5%8F%91%E7%A7%BB%E6%A4%8D%E5%90%8E%E8%BF%98%E4%BC%9A%E6%8E%89%E5%90%97|%E6%A4%8D%E5%8F%91%E5%90%8E%E7%9A%84%E6%B3%A8%E6%84%8F%E4%BA%8B%E9%A1%B9|%E6%A4%8D%E5%8F%91%E8%84%B1%E8%90%BD|%E7%A7%8D%E6%A4%8D%E5%A4%B4%E5%8F%91+%E4%BB%B7%E6%A0%BC|%E4%B8%80%E8%88%AC%E6%A4%8D%E5%8F%91%E5%A4%9A%E5%B0%91%E9%92%B1|%E5%A4%B4%E5%8F%91%E7%A7%8D%E6%A4%8D%E5%8E%BB%E5%93%AA%E9%87%8C|%E6%A4%8D%E5%8F%91%E5%90%8E%E4%B8%89%E4%B8%AA%E6%9C%88|%E5%A4%B4%E5%8F%91%E7%A7%8D%E6%A4%8D%E6%B5%81%E7%A8%8B|%E6%A4%8D%E5%8F%91%E5%A4%A7%E6%A6%82%E9%9C%80%E8%A6%81%E5%A4%9A%E5%B0%91%E9%92%B1|%E5%A4%B4%E5%8F%91%E7%A7%8D%E6%A4%8D%E5%90%8E%E4%BC%9A%E6%8E%89%E5%90%97|%E6%96%B0%E7%94%9F%E6%A4%8D%E5%8F%91%E5%A4%9A%E5%B0%91%E9%92%B1|%E6%A4%8D%E5%8F%91%E5%90%8E%E7%9A%84%E5%A4%B4%E5%8F%91|%E6%A4%8D%E5%8F%91+%E6%80%8E%E4%B9%88%E6%A0%B7|%E7%9B%B4%E5%A4%B4%E5%8F%91|%E5%A4%B4%E5%8F%91%E6%A4%8D%E5%8F%91%E7%9A%84%E8%B4%B9%E7%94%A8|%E5%A4%B4%E5%8F%91%E7%A7%8D%E6%A4%8D+%E5%A4%9A%E5%B0%91%E9%92%B1|%E6%A4%8D%E5%8F%91%E5%90%8E%E8%84%B1%E8%90%BD%E6%9C%9F|%E6%A4%8D%E5%A4%B4%E5%8F%91%E8%B4%B9%E7%94%A8|%E7%BD%91%E4%B8%8A%E6%89%BE%E8%A3%85%E4%BF%AE|%E7%BD%91%E4%B8%8A%E6%89%BE%E8%A3%85%E4%BF%AE|2017%E6%9C%80%E7%83%AD%E9%97%A8%E7%9A%84%E6%89%8B%E6%B8%B8|2017%E6%9C%80%E7%83%AD%E9%97%A8%E7%9A%84%E6%89%8B%E6%B8%B8"

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
      """.stripMargin)

    xianguo_out.write.option("sep", "\t").mode(SaveMode.Append)
      .csv(s"hdfs://master1:9000/sunyj/out/xianguo/$daytime")

    spark.stop()
  }

}

