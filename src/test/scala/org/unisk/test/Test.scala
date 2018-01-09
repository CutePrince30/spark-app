package org.unisk.test

import java.lang.NumberFormatException

import scala.util.Try
import scala.util.control.Exception

/**
  * @author sunyunjie (jaysunyun_361@163.com)
  */

trait Equal {
  def isEqual(x: Any): Boolean

  def isNotEqual(x: Any): Boolean = !isEqual(x)
}

class Point(xc: Int, yc: Int) extends Equal {
  var x: Int = xc
  var y: Int = yc
  override def isEqual(obj: Any): Boolean = {
    obj.isInstanceOf[Point] && obj.asInstanceOf[Point].x == this.x
  }
}



object Test {

  def getUriKey(uri: String): String = {
    if (!uri.isEmpty && uri.contains(".")) {
      val uri_sep = uri.split("\\.")
      uri_sep(0) + "." + uri_sep(1)
    } else {
      uri
    }
  }

  def main(args: Array[String]): Unit = {
//    val point_x = new Point(1, 2)
//    val point_y = new Point(1, 3)
//
//    print(point_x.isEqual(point_y))

//    Try {
//      "a".toInt
//      println("hello")
//    }

    val s = "weixin/?version=369431072&uin=436213475&nettype=0&scene=timeline"
    println(getUriKey(s))
  }
}
