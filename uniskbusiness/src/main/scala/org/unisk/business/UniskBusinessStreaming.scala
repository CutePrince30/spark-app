package org.unisk.business

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  *
  * @author sunyunjie (jaysunyun_361@163.com)
  */
object UniskBusinessStreaming {

  object GetFileNameFromStream extends java.io.Serializable {
    def getFileName(file: RDD[String]): String = {
      file.toDebugString
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: UniskBusinessStreaming <directory>")
      System.exit(1)
    }

    val conf = new SparkConf().setMaster("spark://master:7077")
      .setAppName("NetworkWordCount").set("spark.files.ignoreCorruptFiles", "true")
    val ssc = new StreamingContext(conf, Seconds(args(1).toInt))

    val lines = ssc.textFileStream(args(0))
//    lines.foreachRDD(dataRDD => {
//      val file = GetFileNameFromStream.getFileName(dataRDD)
//
//      /**
//        * (1) MapPartitionsRDD[8] at textFileStream at UniskBusinessStreaming.scala:30 []
// |  UnionRDD[7] at textFileStream at UniskBusinessStreaming.scala:30 []
// |  hdfs://master:9000/data/streaming/test/streaming_test.txt NewHadoopRDD[6] at textFileStream at UniskBusinessStreaming.scala:30 []
//        */
//      println(file)
//    })

    val words = lines.flatMap(_.split(" "))

    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
