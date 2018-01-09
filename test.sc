import scala.util.control.Breaks.{break, breakable}

//val l = List(1, 2, 2, 3, 4, 4, 5, 4, 5, 6, 6, 7, 8, 7, 9, 7, 10)

//val l = List(1, 2, 1, 1, 3, 4, 3, 4, 3, 1)

// (时间, 位置)
val arr = Array[(Int, Int)]((1, 1), (2, 2), (3, 1), (4, 1),
  (5, 3), (6, 4), (7, 3), (15, 4), (16, 3), (17, 1))

//val arr = Array[(Int, Int)]((1, 1))

var i = 0
breakable {
  while (i < arr.length) {
    val f1 = i + 1
    var f2 = i + 2

    if (f1 == arr.length) {
      println(arr(i)._2)
      break
    }

    if (f2 == arr.length) {
      f2 = f1
    }

    if (arr(i)._2 == arr(f1)._2) {
      i = f1
    }
    else {
      if (arr(i)._2 == arr(f2)._2 && arr(f2)._1 - arr(i)._1 < 5) {
        i = f2
      }
      else {
        println(arr(i)._2)
        i = f1
      }
    }
  }
}

//var i = 0
//breakable {
//  while (i < l.length) {
//    val f1 = i + 1
//    var f2 = i + 2
//
//    if (f1 == l.length) {
//      println(l(i))
//      break
//    }
//
//    if (f2 == l.length) {
//      f2 = f1
//    }
//
//    if (l(i) == l(f1)) {
//      i = f1
//    }
//    else {
//      if (l(i) == l(f2)) {
//        i = f2
//      }
//      else {
//        println(l(i))
//        i = f1
//      }
//    }
//  }
//}