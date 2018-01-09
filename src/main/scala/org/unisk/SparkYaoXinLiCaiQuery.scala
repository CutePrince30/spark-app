package org.unisk

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 鲜果广告任务
  *
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object SparkYaoXinLiCaiQuery {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: SparkYaoXinLiCaiQuery <province> <daytime>")
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

    val xianguo_common_df = spark.read.parquet(s"/sunyj/out/uris/business=yaoxin_licai/daytime=$daytime/*.parquet")
    xianguo_common_df.createOrReplaceTempView("licai_common")

    val baidu_xianguo_df = spark.read.parquet(s"/sunyj/out/uris/business=baidu/daytime=$daytime/*.parquet")
    baidu_xianguo_df.createOrReplaceTempView("baidu_licai")
    val baidu_keywords = "%E7%90%86%E8%B4%A2|%E4%BF%A1%E7%94%A8%E5%8D%A1|%E9%93%B6%E8%A1%8C"

    val xianguo_out = sql(
      s"""
         |select
         | msisdn,
         | first(uri),
         | first(refer),
         | first(province)
         |from
         |(
         |	select
         |	 msisdn,
         |	 uri,
         |	 refer,
         |	 province
         |	from
         |	 licai_common
         |
         |	union all
         |
         |  select
         |	 msisdn,
         |	 uri,
         |	 refer,
         |	 province
         |	from
         |	 baidu_licai
         |	where concat(uri, refer) regexp '($baidu_keywords)'
         |) tmp
         |group by msisdn
         |
      """.stripMargin)

    xianguo_out.write.option("sep", "\t").mode(SaveMode.Append)
      .csv(s"hdfs://master1:9000/sunyj/out/yaoxin_licai/$daytime")

    spark.stop()
  }

}
