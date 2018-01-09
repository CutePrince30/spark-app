package org.unisk.test

import org.junit._

@Test
class AppTest {

  @Test
  def testOK(): Unit = {
//    var s =
//      """10.10.10.10 - "FRED" [18/Jan/2013:17:56:07 +1100] "GET http://images.com/2013/Generic.jpg
//        | HTTP/1.1" 304 315 "http://referall.com/" "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1;
//        | GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; .NET CLR
//        | 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR
//        | 3.5.30729; Release=ARP)" "UD-1" - "image/jpeg" "whatever" 0.350 "-" - "" 265 923 934 ""
//        | 62.24.11.25 images.com 1358492167 - Whatup""".stripMargin.lines.mkString
//
//    println(s)
//
//    for (x <- 1 until 10) {
//      println(x)
//    }

//    val v = "csv1"
//    v match {
//      case "csv" => println("OK")
//      case _ => println("NO")
//    }

//    val prop = new Properties
//    val filePath = System.getProperty("user.dir")
//    val conf = new BufferedInputStream(new FileInputStream(filePath + "/src/main/resources/test1.properties"))
//    prop.load(conf)
//
//    println(prop.getProperty("master"))


    var daytime = "20171109"
    var province = "gaungdong"

    var ss =
      s"""
         |select
         |	xh.msisdn as msisdn,
         |	xh.imei as imei,
         |	xh.imsi as imsi,
         |	xh.starttime as starttime,
         |	split(xh.pid, '_')[0] as lac,
         |	split(xh.pid, '_')[1] as ci,
         |	xl.lon as lon,
         |	xl.lat as lat,
         |	xh.province as province,
         |	xl.province as cell_province
         |from
         |	(select
         |		collect_set(imsi)[0] as imsi,
         |		collect_set(imei)[0] as imei,
         |		msisdn,
         |		from_unixtime(unix_timestamp(starttime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHHmmss') as starttime,
         |		concat(lac_tac, '_', ci) as pid,
         |		collect_set(province)[0] as province
         |	from
         |		xdr.http
         |	where province = '$province' and daytime = '$daytime' and msisdn regexp '^(86)?1[^(0|1|2)][0-9]{9}$$' and imei != ''
         |	group by 3,4,5) as xh
         |left join
         |	(select concat(lac, '_', ci) as pid, lon, lat, province
         |	from
         |	  xdr.location
         |	where
         |	  province in ('hubei', 'beijing', 'tianjin', 'hebei', 'guangdong', 'jiangsu', 'zhejiang', 'shanghai') and city = 'all'
         |	) as xl
         |on
         |	xh.pid = xl.pid
         |where
         |	xl.lon is not null and xl.lat is not null
      """.stripMargin

    print(ss)

  }

  //    @Test
  //    def testKO() = assertTrue(false)

}


