package org.unisk.test

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}


/**
  * 北京交委任务
  *
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object SparkTest {

  case class UnicomNum(msisdn: String, imsi: String, imei: String, idfa: String)

  case class People(msisdn: String, imei: String, lac: String)

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

    import spark.implicits._

    val http_df = spark.sql("select msisdn, imei, lac_tac from xdr.http limit 10")
    //    http_df.mapPartitions(iter => iter.toSet.iterator).toDF()


    val unicomDF = spark.sparkContext
      .textFile("hdfs://master1:9000/sunyj/test/unicom.txt")
      .map(_.split("|"))
      .map(attributes => UnicomNum(attributes(0), attributes(1), attributes(2), attributes(3)))
      .toDF()

    unicomDF.groupBy("imei").count().write.mode(SaveMode.Append).option("compression", "gzip").csv("hdfs://master1:9000/sunyj/out/haitao/imei/")


    val df = spark.read.parquet("/sunyj/out/iiim/20171110/*/*.parquet", "/sunyj/out/iiim/20171111/*/*.parquet")
    df.createOrReplaceTempView("iiim")

    //    var fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
//    val xgNumRDD = spark.sparkContext.textFile("/sunyj/test/xgnum.txt")
//    val schemaString = "md5_msisdn"
//    var fields = StructField(name = schemaString, dataType = StringType, nullable = true)
//    val schema = StructType(Seq(fields))
//    val rowRDD = xgNumRDD.map(attribute => Row(attribute))
//    val xgNumDF = spark.createDataFrame(rowRDD, schema)
//    xgNumDF.createOrReplaceTempView("xgnum")

//    spark.sql("select * from xgnum limit 10")
//
//    spark.read.schema(schema).option("sep", "\t").text()
//
//    val loc_df = spark.sql("select concat_ws('_', lac, ci) as pid, concat_ws('_', lon, lat) as ploc from xdr.location")
//
//    val fn = df.schema.fieldNames
//
//    fn.map(field => field -> fields).toMap
//
//    loc_df.rdd.map(record => fn.map(field => field -> record.getAs(field)).toMap).collect()
//
//    val loc_map = loc_df.rdd.map(record => record(0).toString -> record(1).toString).collectAsMap()

    val schemaString = "msisdn imei imsi idfa"
    var fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    spark.read.schema(schema).csv("file:///home/hadoop/data/iiim_test.txt")

    val c = spark.sparkContext.parallelize(1 to 10)
    c.reduce((x, y) => x + y)



    http_df.rdd.mapPartitions(merge).toDF("id", "starttime", "pid", "segment", "province")


    val uris_df = spark.sql("select * from xdr.uris limit 10")

    val uri_map = uris_df.rdd.map(row => {
      val uri_sep = row(0).toString.split("\\.")
      (uri_sep(0) + "." + uri_sep(1), Array(row(0).toString, row(1).toString))
    }).groupByKey().collectAsMap()

    uri_map.get("openapi.haodai").head.foreach(iter => {
      println(iter(0), iter(1))
    })

    uri_map.get("openapi.haodai").head.foreach(iter => {
      println(iter)
    })

    spark.stop()
  }

  //  def merge(iter: Iterator[Row]): Iterator[(String, String)] = {
  //    val r_iter = iter.toSet.iterator
  //    var res = List[(String, String)]()
  //    r_iter.toList.iterator
  //  }


  // 可以的
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

  //
  //
    def merge1(iter: Iterator[Row]): Iterator[(String, String)] = {
      var res = List[(String, String)]()
      if (iter.hasNext) {
        var pre = iter.next
        while (iter.hasNext) {
          val cur = iter.next
          val pre_cols = cur.toSeq.toList
          val cur_cols = cur.toSeq.toList
          println(pre_cols(0), cur_cols(0))
          res :+= (cur_cols(0).toString, cur_cols(1).toString)
          pre = cur
        }
      }
      res.iterator
    }


    def myfunc[T](iter: Iterator[T]): Iterator[(T, T)] = {
      var res = List[(T, T)]()
      var pre = iter.next
      while (iter.hasNext) {
        val cur = iter.next
        res.::=(pre, cur)
        pre = cur
      }
      res.iterator
    }

  //  def myfunc[String](iter: Iterator[String]): Iterator[String] = {
  //    var res = List[String]()
  //    var pre = iter.next
  //    while (iter.hasNext) {
  //      val cur = iter.next
  //      res.::=(cur)
  //      pre = cur
  //    }
  //    res.iterator
  //  }


  //  def myfunc[String](iter: Iterator[String]): Iterator[String] = {
  //    var res = List[String]()
  //    if (iter.hasNext) {
  //      var pre: String = iter.next
  //      while (iter.hasNext) {
  //        val cur: String = iter.next
  //        res.::=(cur)
  //        println(pre, cur.toString.split(",")(0))
  //        pre = cur
  //      }
  //    }
  //    res.iterator
  //  }


}
