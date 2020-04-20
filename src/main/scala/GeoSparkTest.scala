
import java.io.PrintWriter

import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator

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
    // "ST_GeometryType" -> "st_geometry_type.csv",
    // "ST_MakeValid" -> "st_make_valid.csv",
    // "ST_SimplifyPreserveTopology" -> "st_simplify_preserve_topology.csv",
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
    // "ST_NPoints" -> "st_npoints.csv",
    "ST_Envelope" -> "single_polygon.csv",
    "ST_Buffer" -> "single_point.csv",
    "ST_Union_Aggr" -> "single_polygon.csv",
    // "ST_Envelope_Aggr" -> "st_envelope_aggr.csv",
    "ST_Transform" -> "single_point.csv",
    // "ST_CurveToLine" -> "st_curvetoline.csv",
    "ST_GeomFromWkt" -> "single_polygon.csv",
    "ST_GeomFromGeojson" -> "st_geomfromgeojson.csv",
    "ST_AsText" -> "single_polygon.csv"
  )
  // register geospark
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  var sparkSession: SparkSession = SparkSession.builder().config("spark.serializer", classOf[KryoSerializer].getName).
    config("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName).
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

    //    println("print configs.....")
    //    println("input csv path : "+resourceFolder)
    //    println("output log path : "+outputFolder)
    //    println("test funtion name : "+testFunc)
    //    println("test func list : "+funcList)
    //    println("test times per function : "+perfTestTimes)


    // perf tests
    import scala.collection.mutable.ArrayBuffer
    var perfLogArr = ArrayBuffer[Double]()
    var errorFlag = false

    // test specific func
    val curFunc = testFunc.toString()
    if (funcCsvMap.contains(curFunc)) {
      for (i <- 1 to perfTestTimes) {
        val func = Util.getFunc(curFunc)
        if (func != -1.0) {
          perfLogArr += func()
        } else {
          println("No this function...........")
        }
      }
      Util.perfLog(curFunc, perfLogArr)
    }

    // test all funcs
    else if (curFunc == "all") {
      println("Test all functions .........")
      val keys = funcCsvMap.keySet
      for (key <- keys) {
        var cur = key
        for (i <- 1 to perfTestTimes) {
          val func = Util.getFunc(cur)
          if (func != 0.0) {
            perfLogArr += func()
          } else {
            println("No this function...........")
          }
        }
        assert(perfLogArr.length == perfTestTimes)
        Util.perfLog(cur, perfLogArr)
        perfLogArr.clear()

      }
    } else {
      println("ERROR : Test function " + curFunc + " not exist!")
    }

    println(argsMap)
    //   ST_Point_test()
    //   ST_Intersection_test()
    //   ST_IsValid_test()
    //   ST_Equals_test()
    //   ST_Touches_test()
    //   ST_Overlaps_test()
    //   ST_Crosses_test()
    //   ST_IsSimple_test()
    //   // ST_GeometryType_test() // error
    //   // ST_MakeValid_test() // error
    //   // ST_SimplifyPreserveTopology_test() // error
    //   ST_PolygonFromEnvelope_test()
    //   ST_Contains_test()
    //   ST_Intersects_test()
    //   ST_Within_test()
    //   ST_Distance_test()
    //   ST_Area_test()
    //   ST_Length_test()
    //   // ST_HausDorffDistance_test // *
    //   ST_ConvexHull_test()
    //   // ST_NPoints_test() // error
    //   ST_Envelope_test()
    //   ST_Buffer_test()
    //   ST_Union_Aggr_test()
    //   ST_Transform_test()
    //   // ST_CurveToLine_test // *
    //   ST_GeomFromGeojson_test()
    //   ST_GeomFromWkt_test()
    //   ST_AsText_test()
    //   ST_Centroid_test()
  }

  def ST_Point_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_Point")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_Point(cast(test._c0 as Decimal(24,20)),cast(test._c0 as Decimal(24,20))) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")

    end = System.currentTimeMillis
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  def ST_Intersection_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_Intersection")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_Intersection(ST_GeomFromWkt(test._c0),ST_GeomFromWkt(test._c1)) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  def ST_IsValid_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_IsValid")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_IsValid(ST_GeomFromWkt(test._c0)) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    //// outputDf.show(false)
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  def ST_Equals_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_Equals")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_Equals(ST_GeomFromWkt(test._c0),ST_GeomFromWkt(test._c1)) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  def ST_Touches_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_Touches")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_Touches(ST_GeomFromWkt(test._c0),ST_GeomFromWkt(test._c1)) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  def ST_Overlaps_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_Overlaps")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_Overlaps(ST_GeomFromWkt(test._c0),ST_GeomFromWkt(test._c1)) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  def ST_Crosses_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_Crosses")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_Crosses(ST_GeomFromWkt(test._c0),ST_GeomFromWkt(test._c1)) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  def ST_IsSimple_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_IsSimple")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_IsSimple(ST_GeomFromWkt(test._c0)) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  // geospark can not work
  def ST_GeometryType_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_GeometryType")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_GeometryType(ST_GeomFromWkt(test._c0)) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  //geospark can not work
  def ST_MakeValid_test(): Double = {
    import org.geotools.geometry.jts.JTS.makeValid
    var csvPath = resourceFolder + funcCsvMap("ST_MakeValid")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_MakeValid(ST_GeomFromWkt(test._c0),false) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  // geospark can not work
  def ST_SimplifyPreserveTopology_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_SimplifyPreserveTopology")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_SimplifyPreserveTopology(ST_GeomFromWkt(test._c0),10.0) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  def ST_PolygonFromEnvelope_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_PolygonFromEnvelope")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_PolygonFromEnvelope(cast(test._c0 as Decimal(24,20)),cast(test._c1 as Decimal(24,20)),cast(test._c2 as Decimal(24,20)),cast(test._c3 as Decimal(24,20))) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  def ST_Contains_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_Contains")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_Contains(ST_GeomFromWkt(test._c0),ST_GeomFromWkt(test._c1)) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  def ST_Intersects_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_Intersects")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_Intersects(ST_GeomFromWkt(test._c0),ST_GeomFromWkt(test._c1)) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  def ST_Within_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_Within")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_Within(ST_GeomFromWkt(test._c0),ST_GeomFromWkt(test._c1)) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  def ST_Distance_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_Distance")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_Distance(ST_GeomFromWkt(test._c0),ST_GeomFromWkt(test._c1)) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    //// outputDf.show(false)
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  def ST_Area_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_Area")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_Area(ST_GeomFromWkt(test._c0)) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  def ST_Centroid_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_Centroid")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_Centroid(ST_GeomFromWkt(test._c0)) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    //    var tab = inputDf.crossJoin(inputDf)
    //    tab.show(false)
    //    val schemas = Seq("c1","c2")
    //    tab.toDF(schemas:_*).limit(100000).coalesce(1).write.csv("csv/two")
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  def ST_Length_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_Length")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_Length(ST_GeomFromWkt(test._c0)) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  // geospark doesn't have this func
  def ST_HausDorffDistance_test(): Double = {
    return -1.0
  }

  def ST_ConvexHull_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_ConvexHull")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_ConvexHull(ST_GeomFromWkt(test._c0)) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  def ST_NPoints_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_NPoints")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_NPoints(ST_GeomFromWkt(test._c0)) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  def ST_Envelope_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_Envelope")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_Envelope(ST_GeomFromWkt(test._c0)) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  def ST_Buffer_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_Buffer")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_Buffer(ST_GeomFromWkt(test._c0),1.8) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  def ST_Union_Aggr_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_Union_Aggr")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath).limit(10)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_Union_Aggr(ST_GeomFromWkt(test._c0)) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  // geospark doesn't have this func
  def ST_Envelope_Aggr_test(): Double = {
    return -1.0
  }

  def ST_Transform_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_Transform")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath).limit(10)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_Transform(ST_GeomFromWkt(test._c0), 'epsg:3857','epsg:4326') from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  // geospark doesn't have this func
  def ST_CurveToLine_test(): Double = {
    return -1.0
  }

  def ST_GeomFromGeojson_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_GeomFromGeojson")
    var inputDf = sparkSession.read.option("delimiter", "\t").option("header", "false").csv(csvPath).limit(10)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_GeomFromGeojson(test._c0) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  def ST_GeomFromWkt_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_GeomFromWkt")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath).limit(10)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_GeomFromWkt(test._c0) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }

  def ST_AsText_test(): Double = {
    var csvPath = resourceFolder + funcCsvMap("ST_AsText")
    var inputDf = sparkSession.read.option("delimiter", "|").option("header", "false").csv(csvPath)
    inputDf.cache()
    inputDf.createOrReplaceTempView("test")
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var outputDf = sparkSession.sql("select ST_GeomFromWkt(test._c0) from test")
    // outputDf.collect()
    outputDf.createOrReplaceTempView("outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    sparkSession.sql("CACHE TABLE outputDf")
    end = System.currentTimeMillis
    if (GeoSparkTest.showFlag) {
      outputDf.show(false)
    }
    var cost = (end - start) / 1000.0
    printf("Run Time (collect)= %f[s]\n", cost)
    return cost
  }
}

object Util {
  val outputBaseDir = GeoSparkTest.outputFolder.toString()
  val hdfs_path = "hdfs://output/"
  val to_hdfs = false

  def perfLog(funcName: String, perfRes: scala.collection.mutable.ArrayBuffer[Double]): Unit = {
    var logfileName = ""
    if (to_hdfs) {
      logfileName = hdfs_path + funcName + ".log"
    } else {
      logfileName = outputBaseDir + funcName + ".log"
    }

    import java.io._
    import java.text.SimpleDateFormat
    import java.util.Date
    val file = new PrintWriter(new File(logfileName))
    for (perfTime <- perfRes) {
      file.write(new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date()) + "    " + perfTime + " [s]\n")
    }
    file.close()
  }

  def getFunc(funcString: String): () => Double = {
    () => {
      funcString match {
        case "ST_Point" => GeoSparkTest.ST_Point_test()
        case "ST_Intersection" => GeoSparkTest.ST_Intersection_test()
        case "ST_IsValid" => GeoSparkTest.ST_IsValid_test()
        case "ST_Equals" => GeoSparkTest.ST_Equals_test()
        case "ST_Touches" => GeoSparkTest.ST_Touches_test()
        case "ST_Overlaps" => GeoSparkTest.ST_Overlaps_test()
        case "ST_Crosses" => GeoSparkTest.ST_Crosses_test()
        case "ST_IsSimple" => GeoSparkTest.ST_IsSimple_test()
        case "ST_GeomtryType" => GeoSparkTest.ST_GeometryType_test()
        case "ST_MakeValid" => GeoSparkTest.ST_MakeValid_test()
        case "ST_SimplifyPreserveTopology" => GeoSparkTest.ST_SimplifyPreserveTopology_test()
        case "ST_PolygonFromEnvelope" => GeoSparkTest.ST_PolygonFromEnvelope_test()
        case "ST_Contains" => GeoSparkTest.ST_Contains_test()
        case "ST_Intersects" => GeoSparkTest.ST_Intersects_test()
        case "ST_Within" => GeoSparkTest.ST_Within_test()
        case "ST_Distance" => GeoSparkTest.ST_Distance_test()
        case "ST_Area" => GeoSparkTest.ST_Area_test()
        case "ST_Centroid" => GeoSparkTest.ST_Centroid_test()
        case "ST_Length" => GeoSparkTest.ST_Length_test()
        case "ST_HausDorffDistance" => GeoSparkTest.ST_HausDorffDistance_test()
        case "ST_ConvexHull" => GeoSparkTest.ST_ConvexHull_test()
        case "ST_NPoints" => GeoSparkTest.ST_NPoints_test()
        case "ST_Envelope" => GeoSparkTest.ST_Envelope_test()
        case "ST_Buffer" => GeoSparkTest.ST_Buffer_test()
        case "ST_Union_Aggr" => GeoSparkTest.ST_Union_Aggr_test()
        case "ST_Envelope_Aggr" => GeoSparkTest.ST_Envelope_Aggr_test()
        case "ST_Transform" => GeoSparkTest.ST_Transform_test()
        case "ST_CurveToLine" => GeoSparkTest.ST_CurveToLine_test()
        case "ST_GeomFromGeojson" => GeoSparkTest.ST_GeomFromGeojson_test()
        case "ST_GeomFromWkt" => GeoSparkTest.ST_GeomFromWkt_test()
        case "ST_AsText" => GeoSparkTest.ST_AsText_test()
        //        case _ => Option.empty
        case _ => -1.0
      }
    }
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
