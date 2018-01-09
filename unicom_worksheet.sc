import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

//var l = List(1, 2, 2, 3, 4, 5, 4, 5, 9, 4, 6, 7, 8, 7, 9)
//
//var s = ListBuffer[Int]()
//
//var p1 = 0
//s.append(l(p1))
//for (i <- 1 until l.length) {
//  if (l(p1) != l(i)) {
//    p1 = i
//    s.append(l(p1))
//  }
//}
//
//val z = ListBuffer[Int]()
//var j = 0
//
//breakable {
//  while (j < s.length) { // 12
//    if (j == s.length - 2) { // 正常匹配到了倒数第二个值
//      z.append(s(j))
//      z.append(s(j + 1))
//      break
//    }
//    if (j == s.length - 1) { // 倒数第三个和倒数第一个是一样的
//      z.append(s(j))
//      break
//    }
//
//    if (s(j) != s(j + 2)) {
//      z.append(s(j))
//      j = j + 1
//    }
//    else {
//      j = j + 2
//    }
//  }
//}
//
//z

//           p1 i
val l = List(1, 2, 2, 3, 4, 4, 5, 4, 5, 9, 4, 6, 7, 8, 7, 9)

// i和i+1不重复，就和i+2比较一下

//var s = ListBuffer[Int]()
//var p1 = 0
//s.append(l(p1))
//
//for (i <- 1 until l.length) {
//  if (l(p1) != l(i)) {
//    p1 = i
//    s.append(l(p1))
//  } else {
//
//  }
//}
//
//
//for (i <- 1 until l.length) {
//  if (l(p1) != l(i)) {
//    p1 = i
//  }
//}

var i = 0
breakable {
  while(i < l.length) {
    var f1 = i + 1
    var f2 = i + 2

    if (f1 == l.length) {
      print(l(f1))
      break
    }

    if (f2 == l.length) {
      f2 = f1
    }

    if (i == f1) {
      i = f1
    }
    else {
      if (i == f2) {
        i = f2
      }
      else {
        print(l(i))
        i = f1
      }
    }
  }
}



//val uri = "ba.xcar.com.cn/askedprice/39/a.html"
//val s = "a.xcar.com.cn/askedprice|/39/|a.html"

//def concat_fields(log: String): String = {
//  if (log.startsWith("\"") && log.endsWith("\"")) {
//    log.drop(1).dropRight(1)
//  }
//  else {
//    log
//  }
//}
//
//val host = "\"hotsoon.snssdk.com\""
//var url = "/hotsoon/gift/?live_sdk_version"
//
//concat_fields(host)

//if (s.contains("|")) {
//  val regexp = ("^" + s.replaceAll("\\|", ".*")).r
//  println(regexp)
//  regexp.findFirstIn(uri)
//}

//import scala.collection.mutable.ListBuffer
////val pattern="Scalable".r
////
////val str = "Scala is Scalable and cool"
////
////print(pattern.findFirstIn(str))
//
//var z = ListBuffer[(String, String)]()
//
//z += (("a", "b"))
//
//z += (("b", "c"))
//
//z.append(("c", "d"))
//
//z
//
//var set = scala.collection.mutable.Set[(String, String)]()
//
//set add (("a", "b"))
//
//set add (("a", "b"))
//
//set += (("b", "c"))


//import scala.collection.mutable.ListBuffer
//
//var l = ListBuffer("a")
//l += "b"
//var l1 = l.toSet
//
//var z = ListBuffer("b")
//z += "c"
//var z1 = z.toSet
//
//var list = ListBuffer[(String, String)]()
//list :+= ("a", "b")
//list :+= ("c", "d")
//var list1 = list.toSet
//
//
//l1.intersect(z1)


//val site1 = "Runoob" :: ("Google" :: ("Baidu" :: Nil))
//var site2 = "Facebook" :: ("Taobao" :: Nil)
//
//site2.:+("a")
//site2.+:("b")




//val s = "1.2.3"
//val uri_sep = s.split("\\.")
//uri_sep(0) + "." + uri_sep(1)

//var fun1 = (x: Int, y: Int) => x + y
//fun1(1, 2)
//
//def greeting = (name: String) => {
//  s"Hello $name"
//}
//greeting("Jeremy")
//
//def curriedAdd(x: Int)(y: Int) = x + y
//curriedAdd(1)(2)

//def factorial(n: Int): Int =
//  if (n <= 0) 1
//  else n * factorial(n - 1)

//@annotation.tailrec
//def factorial(n: Int, m: Int): Int =
//  if (n <= 0) m
//  else factorial(n - 1, n * m)


//object SumFunc {
//
//  def sum(f: Int => Int)(a: Int)(b: Int): Int = {
//
//    @annotation.tailrec
//    def loop(n: Int, acc: Int): Int = {
//      if (n > b) {
//        println(s"n=${n}, acc=${acc}")
//        acc
//      } else {
//        println(s"n=${n}, acc=${acc}")
//        loop(n + 1, f(n) + acc)
//      }
//    }
//
//    loop(a, 0)
//  }
//
//  val SumSquare = sum(x => x)_
//  SumSquare(1)(3)
//
//}

//var s: String = "116.23657227"
//s.toDouble
//Long.MaxValue

