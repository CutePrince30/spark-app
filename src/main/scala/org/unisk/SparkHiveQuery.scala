package org.unisk

import java.io.{BufferedInputStream, FileInputStream}
import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object SparkHiveQuery {

  val prop = new Properties

  def initProperties(conf_file: String): Unit = {
    val conf = new BufferedInputStream(new FileInputStream(conf_file))
    prop.load(conf)
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.println("Usage: SparkHiveQuery <configuration file>")
      System.exit(1)
    }
    initProperties(args(0))

    val spark = SparkSession
      .builder()
      .master(prop.getProperty("master"))
      .appName(prop.getProperty("app_name"))
      .config("spark.sql.warehouse.dir", prop.getProperty("spark_sql_warehouse_dir"))
      .config("spark.files.ignoreCorruptFiles", value = true)
      .config("hive.exec.compress.output", value = true)
      .config("mapred.output.compress", value = true)
      .config("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.groupby.orderby.position.alias", value = true)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    val df = sql(prop.getProperty("sql"))

    val write_dest = prop.getProperty("write_dest")

    val write_type = prop.getProperty("write_type")
    write_type match {
      case "csv" => df.write.mode(SaveMode.Append).csv(write_dest)
      case "parquet" => df.write.mode(SaveMode.Append).parquet(write_dest)
      case "mysql" => df.write.jdbc(prop.getProperty("mysql_url"), prop.getProperty("mysql_table"), new Properties)
      case "hive" => df.write.insertInto(prop.getProperty("hive_table"))
      case _ => System.err.println("Not write type found: " + write_type)
    }

    spark.stop()
  }

}
