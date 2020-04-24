
import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
// import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
// import org.datasyslab.geospark.spatialRDD.SpatialRDD
// import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
// import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator

import java.io.PrintWriter
import java.io._

import scala.collection.mutable.ArrayBuffer

object GeoSparkTest {

  // default configs
  val funcList: ArrayBuffer[String] = ArrayBuffer("ST_Point", "ST_IsValid", "ST_Within")
  var resourceFolder: StringBuilder = new StringBuilder(System.getProperty("user.dir") + "/csv/")
  var outputFolder: StringBuilder = new StringBuilder(System.getProperty("user.dir") + "/output/")
  var testFunc: StringBuilder = new StringBuilder()
  var showFlag = false
  var perfTestTimes: Int = 6

  val funcCsvMap = Map(
    "ST_Point" -> "st_point.csv",
    "ST_Intersection" -> "double_col.csv",
    "ST_IsValid" -> "single_col.csv",
    "ST_Equals" -> "double_col.csv",
    "ST_Touches" -> "double_col.csv",
    "ST_Overlaps" -> "double_col.csv",
    "ST_Crosses" -> "double_col.csv",
    "ST_IsSimple" -> "single_polygon.csv",
    "ST_GeometryType" -> "single_col.csv",
    // "ST_MakeValid" -> "single_polygon.csv", // Currently arctern is limited by the spark udf one-to-one mechanism to not be able to test the function
    "ST_SimplifyPreserveTopology" -> "single_col.csv",
    "ST_PolygonFromEnvelope" -> "st_polygon_from_envelope.csv",
    "ST_Contains" -> "double_col.csv",
    "ST_Intersects" -> "double_col.csv",
    "ST_Within" -> "st_within.csv",
    "ST_Distance" -> "st_distance.csv",
    "ST_Area" -> "single_polygon.csv",
    "ST_Centroid" -> "single_col.csv",
    "ST_Length" -> "single_linestring.csv",
    // "ST_HausdorffDistance" -> "st_hausdorffdistance.csv",
    "ST_ConvexHull" -> "single_col.csv",
    "ST_NPoints" -> "single_col.csv",
    "ST_Envelope" -> "single_polygon.csv",
    "ST_Buffer" -> "single_point.csv",
    "ST_Union_Aggr" -> "single_polygon.csv",
    "ST_Envelope_Aggr" -> "single_polygon.csv",
    "ST_Transform" -> "single_point.csv",
    // "ST_CurveToLine" -> "st_curvetoline.csv",
    "ST_GeomFromWkt" -> "single_polygon.csv",
    "ST_GeomFromGeojson" -> "st_geomfromgeojson.csv",
    "ST_AsText" -> "single_polygon.csv"
  )
  val funcList1 = List(
    "ST_IsValid",
    "ST_IsSimple" ,
    "ST_GeometryType" ,
    // "ST_MakeValid", // Currently arctern is limited by the spark udf one-to-one mechanism to not be able to test the function
    "ST_SimplifyPreserveTopology" ,
    "ST_Transform",
    "ST_Buffer",
    "ST_Area" ,
    "ST_Centroid" ,
    "ST_Length" ,
    "ST_ConvexHull" ,
    "ST_NPoints" ,
    "ST_Envelope" ,
    "ST_Union_Aggr" ,
    "ST_Envelope_Aggr" ,
    "ST_AsText"
  )
  val funcList2 = List(
    "ST_Intersection",
    "ST_Equals" ,
    "ST_Touches" ,
    "ST_Overlaps" ,
    "ST_Crosses" ,
    "ST_Contains" ,
    "ST_Intersects",
    "ST_Within" ,
    "ST_Distance"
  )
  val funcList3 = List(
    "ST_Point",
    "ST_PolygonFromEnvelope"
  )
  // register geospark
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  var sparkSession: SparkSession = SparkSession.builder().config("spark.serializer", classOf[KryoSerializer].getName).
    //config("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName).
    appName("GeoSparkSQL-demo").getOrCreate()

  GeoSparkSQLRegistrator.registerAll(sparkSession)

  def main(args: Array[String]) {
    // parser console args
    val arglist = args.toList
    val argsMap = Parser.nextOption(Map(), arglist)
    println("The args Map : " + argsMap)
    if (argsMap.contains('inputCsvPath)) {
      resourceFolder = new StringBuilder(argsMap('inputCsvPath)).append("/")
    }
    if (argsMap.contains('outputLogPath)) {
      outputFolder = new StringBuilder(argsMap('outputLogPath)).append("/")
    }
    if (argsMap.contains('funcName)) {
      testFunc = new StringBuilder(argsMap('funcName))
      if (!funcList.contains(argsMap('funcName)))
        funcList += testFunc.toString
    }
    if (argsMap.contains('perfTestTimes)) {
      perfTestTimes = argsMap('perfTestTimes).toInt
    }
    if (argsMap.contains('help)) {
      println(argsMap('help))
    }
    if (argsMap.contains('showFlag) && argsMap('showFlag).equals(true.toString())) {
      GeoSparkTest.showFlag = true
    }
    if (argsMap.isEmpty) {
      println("Use default configs........")
    }
    // perf tests
    import scala.collection.mutable.ArrayBuffer
    var perfLogArr = ArrayBuffer[Double]()
    val curFunc = testFunc.toString()

    if (funcList1.exists(x=>x==curFunc)) {
      println("Test +"+curFunc+" .........")
      for (i <- 1 to perfTestTimes) {
        perfLogArr += gisTest1(curFunc)
      }
      Util.perfLog(curFunc, perfLogArr)
    }
    else if (funcList2.exists(x=>x==curFunc)){
      println("Test +"+curFunc+" .........")
      for (i <- 1 to perfTestTimes) {
        perfLogArr += gisTest2(curFunc)
      }
      Util.perfLog(curFunc, perfLogArr)
    }
    else if (funcList3.exists(x=>x==curFunc)){
      println("Test "+curFunc+" .........")
      for (i <- 1 to perfTestTimes) {
        perfLogArr += gisTest3(curFunc)
      }
      Util.perfLog(curFunc, perfLogArr)
    }
    else if (curFunc == "all") {
      println("Test all functions .........")
      perfLogArr.clear()
      for (func1 <- funcList1) {
        for (i <- 1 to perfTestTimes) {
          perfLogArr += gisTest1(func1)
        }
        assert(perfLogArr.length == perfTestTimes)
        Util.perfLog(curFunc, perfLogArr)
        perfLogArr.clear()
      }
      for (func2 <- funcList2) {
        for (i <- 1 to perfTestTimes) {
          perfLogArr += gisTest2(func2)
        }
        assert(perfLogArr.length == perfTestTimes)
        Util.perfLog(curFunc, perfLogArr)
        perfLogArr.clear()
      }
      for (func3 <- funcList3) {
        for (i <- 1 to perfTestTimes) {
          perfLogArr += gisTest3(func3)
        }
        assert(perfLogArr.length == perfTestTimes)
        Util.perfLog(curFunc, perfLogArr)
        perfLogArr.clear()
      }
    }
    else {
      println("ERROR : Test function " + curFunc + " not exist!")
    }

    println(argsMap)
  }

  def gisTest1(funName:String): Double = {
    var csvPath = resourceFolder + funcCsvMap(funName)
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("wkt")
    var tmpDf = sparkSession.sql("select ST_GeomFromWkt(wkt._c0) as wkb1 from wkt")
    tmpDf.createOrReplaceTempView("test")
    sparkSession.sql("CACHE TABLE test")

    var sqlStr = "select "+funName+"(test.wkb1) from test"
    if(funName == "ST_SimplifyPreserveTopology") {
      sqlStr = "select "+funName+"(test.wkb1,10.0) from test"
    }else if (funName == "ST_Transform") {
      sqlStr = "select "+funName+"(test.wkb1, 'epsg:3857','epsg:4326') from test"
    }else if (funName == "ST_Buffer"){
      sqlStr = "select "+funName+"(test.wkb1,1.2) from test"
    }

    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql(sqlStr)
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("UNCACHE TABLE outputDf")
    end = System.currentTimeMillis
    var cost = (end - start) / 1000.0
    if (GeoSparkTest.showFlag) {
      //      if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }
  def gisTest2(funName:String): Double = {
    var csvPath = resourceFolder + funcCsvMap(funName)
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("wkt")
    var tmpDf = sparkSession.sql("select ST_GeomFromWkt(wkt._c0) as wkb1,ST_GeomFromWkt(wkt._c1) as wkb2 from wkt")
    tmpDf.createOrReplaceTempView("test")
    sparkSession.sql("CACHE TABLE test")

    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select "+funName+"(test.wkb1,test.wkb2) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("UNCACHE TABLE outputDf")
    end = System.currentTimeMillis
    var cost = (end - start) / 1000.0
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }
  def gisTest3(funName:String): Double = {
    var csvPath = resourceFolder + funcCsvMap(funName)
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")

    var sqlStr = "select ST_PolygonFromEnvelope(cast(test._c0 as Decimal(24,20)),cast(test._c1 as Decimal(24,20)),cast(test._c2 as Decimal(24,20)),cast(test._c3 as Decimal(24,20))) from test"
    if (funName == "ST_Point"){
      sqlStr = "select ST_Point(cast(test._c0 as Decimal(24,20)),cast(test._c0 as Decimal(24,20))) from test"
    }
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql(sqlStr)
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("UNCACHE TABLE outputDf")
    end = System.currentTimeMillis
    var cost = (end - start) / 1000.0
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }
}

object Util {
  val outputBaseDir = GeoSparkTest.outputFolder.toString()
  var to_hdfs = false
  if( GeoSparkTest.outputFolder.startsWith("hdfs") ){
    to_hdfs = true
  }
  def perfLog(funcName: String, perfRes: scala.collection.mutable.ArrayBuffer[Double]): Unit = {
    var logfileName = outputBaseDir + funcName + ".log"
    var myFileStream : MyFS = if (to_hdfs) new MyHDFS(outputBaseDir) else new MyLocalFS(outputBaseDir)
    myFileStream.myWrite(funcName,perfRes.toArray)

//    for (perfTime <- perfRes) {
//      file.write(new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date()) + "    " + perfTime + " [s]\n")
//    }
  }
}

object Parser {
  val usage =
    """
    Usage: spark-submit --master=yarn xx.jar [-h] [-p] <inputCsvPath> [-o] <outputLogPath> [-f] <testFuncName>
  """
  type OptionMap = Map[Symbol, String]

  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    list match {
      case Nil => map
      case "-h" :: other =>
        nextOption(map ++ Map('help -> usage), other)
      case "-p" :: value :: tail =>
        nextOption(map ++ Map('inputCsvPath -> value.toString), tail)
      case "-o" :: value :: tail =>
        nextOption(map ++ Map('outputLogPath -> value.toString), tail)
      case "-f" :: value :: tail =>
        nextOption(map ++ Map('funcName -> value.toString), tail)
      case "-t" :: value :: tail =>
        nextOption(map ++ Map('perfTestTimes -> value.toString), tail)
      case "-s" :: tail =>
        nextOption(map ++ Map('showFlag -> true.toString), tail)
      case option :: tail =>
        nextOption(map, tail)
    }
  }
}

class MyFS() {
  def myWrite(funcName: String, durTimeArray: Array[Double]): Unit = {
  }
}

class MyLocalFS(outputPath: String) extends MyFS {
  val realPath = outputPath

  def mkdirDirectory(): Unit = {
    val fp = new File(realPath)
    if (!fp.exists()) {
      fp.mkdirs()
    }
  }

  mkdirDirectory()

  override def myWrite(funcName: String, durTimeArray: Array[Double]): Unit = {
    val writer = new PrintWriter(realPath + funcName + ".txt")
    val i = 0
    for (i <- 0 to durTimeArray.length - 1) {
      writer.println("geospark_" + funcName + "_time:" + durTimeArray(i))
    }
    writer.close()
  }
}

class MyHDFS(outputPath: String) extends MyFS {
  var hdfsPath = "hdfs://spark1:9000"
  var realPath: String = "/test_data/"

  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.fs.FileSystem
  import org.apache.hadoop.fs.Path
  def parseHDFS(): Unit = {
    var strList = outputPath.split("/", -1)
    hdfsPath = strList(0) + "//" + strList(2)
    realPath = "/" + strList.takeRight(strList.length - 3).mkString("/")
  }

  parseHDFS()

  val conf = new Configuration()
  conf.set("fs.defaultFS", hdfsPath)
  val fs = FileSystem.get(conf)

  def mkdirDirectory(): Unit = {
    if (!fs.exists(new Path(realPath))) {
      fs.mkdirs(new Path(realPath))
    }
  }

  mkdirDirectory()

  override def myWrite(funcName: String, durTimeArray: Array[Double]): Unit = {
    val output = fs.create(new Path(realPath + funcName + ".txt"))
    val writer = new PrintWriter(output)
    try {
      val i = 0
      for (i <- 0 to durTimeArray.length - 1) {
        writer.println("geospark_" + funcName + "_time:" + durTimeArray(i))
      }
    }
    finally {
      writer.close()
    }
  }
}
