
import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator

object GeoSparkDemo extends App{
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

	var sparkSession:SparkSession = SparkSession.builder().config("spark.serializer",classOf[KryoSerializer].getName).
		config("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName).
		master("local[*]").appName("GeoSparkSQL-demo").getOrCreate()

	GeoSparkSQLRegistrator.registerAll(sparkSession)

  println("This is GeoSpark test demo by czp.")
  val funcList: List[String] = List("ST_Point", "ST_IsValid", "ST_Within")
	val resourceFolder = System.getProperty("user.dir")+"/src/test/resources/"

  // * //val csvPolygonInputLocation = resourceFolder + "testenvelope.csv"
  val csvPolygonInputLocation = "polygon1m.csv"
  val csvPointInputLocation = resourceFolder + "testpoint.csv"
  val shapefileInputLocation = resourceFolder + "shapefiles/dbf"

  testGeoSpark()


  def testGeoSpark():Unit =
  {
    val geosparkConf = new GeoSparkConf(sparkSession.sparkContext.getConf)
    println(geosparkConf)

    // var polygonCsvDf = sparkSession.read.format("csv").option("delimiter",",").option("header","false").load(csvPolygonInputLocation)
    var polygonCsvDf = sparkSession.read.option("delimiter",",").option("header","false").csv(csvPolygonInputLocation)
    polygonCsvDf.cache()
    polygonCsvDf.createOrReplaceTempView("polygontable")
    // polygonCsvDf.show()
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    //  //var polygonDf1 = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
    var polygonDf1 = sparkSession.sql("select ST_Centroid(ST_GeomFromWkt(polygontable._c0)) as polygonshape from polygontable")
    // polygonDf.createOrReplaceTempView("polygondf")
    polygonDf1.collect()
    end = System.currentTimeMillis
    printf("Run Time (collect)= %f[s]\n", (end - start) / 1000.0)

    start = System.currentTimeMillis
    //var polygonDf2 = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
    var polygonDf2 = sparkSession.sql("select ST_Centroid(ST_GeomFromWkt(polygontable._c0)) as polygonshape from polygontable")
    // polygonDf.createOrReplaceTempView("polygondf")
    // polygonDf2.foreach(foreachFunc)
    polygonDf2.count()
    end = System.currentTimeMillis
    printf("Run Time (count)= %f[s]\n", (end - start) / 1000.0)

    start = System.currentTimeMillis
    //var polygonDf3 = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
    var polygonDf3 = sparkSession.sql("select ST_Centroid(ST_GeomFromWkt(polygontable._c0)) as polygonshape from polygontable")
    // polygonDf.createOrReplaceTempView("polygondf")
    polygonDf3.take(1)
    end = System.currentTimeMillis
    printf("Run Time (take(1))= %f[s]\n", (end - start) / 1000.0)

    start = System.currentTimeMillis
    //var polygonDf334 = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
    var polygonDf3_1 = sparkSession.sql("select ST_Centroid(ST_GeomFromWkt(polygontable._c0)) as polygonshape from polygontable")
    // polygonDf.createOrReplaceTempView("polygondf")
    polygonDf3_1.take(100000)
    end = System.currentTimeMillis
    printf("Run Time (take(100000))= %f[s]\n", (end - start) / 1000.0)

    println()
// 2
    start = System.currentTimeMillis
    //var polygonDf11 = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
    var polygonDf11 = sparkSession.sql("select ST_Centroid(ST_GeomFromWkt(polygontable._c0)) as polygonshape from polygontable")
    // polygonDf.createOrReplaceTempView("polygondf")
    polygonDf11.collect()
    end = System.currentTimeMillis
    printf("Run Time (collect)= %f[s]\n", (end - start) / 1000.0)

    start = System.currentTimeMillis
    //var polygonDf22 = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
    var polygonDf22 = sparkSession.sql("select ST_Centroid(ST_GeomFromWkt(polygontable._c0)) as polygonshape from polygontable")
    // polygonDf.createOrReplaceTempView("polygondf")
    // polygonDf2.foreach(foreachFunc)
    polygonDf22.count()
    end = System.currentTimeMillis
    printf("Run Time (count)= %f[s]\n", (end - start) / 1000.0)

    start = System.currentTimeMillis
    //var polygonDf33 = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
    var polygonDf33 = sparkSession.sql("select ST_Centroid(ST_GeomFromWkt(polygontable._c0)) as polygonshape from polygontable")
    // polygonDf.createOrReplaceTempView("polygondf")
    polygonDf33.take(1)
    end = System.currentTimeMillis
    printf("Run Time (take(1))= %f[s]\n", (end - start) / 1000.0)

    start = System.currentTimeMillis
    //var polygonDf335 = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
    var polygonDf335 = sparkSession.sql("select ST_Centroid(ST_GeomFromWkt(polygontable._c0)) as polygonshape from polygontable")
    // polygonDf.createOrReplaceTempView("polygondf")
    polygonDf335.take(100000)
    end = System.currentTimeMillis
    printf("Run Time (take(100000))= %f[s]\n", (end - start) / 1000.0)

    println()
// 3
    start = System.currentTimeMillis
    //var polygonDf111 = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
    var polygonDf111 = sparkSession.sql("select ST_Centroid(ST_GeomFromWkt(polygontable._c0)) as polygonshape from polygontable")
    // polygonDf.createOrReplaceTempView("polygondf")
    polygonDf111.collect()
    end = System.currentTimeMillis
    printf("Run Time (collect)= %f[s]\n", (end - start) / 1000.0)

    start = System.currentTimeMillis
    //var polygonDf222 = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
    var polygonDf222 = sparkSession.sql("select ST_Centroid(ST_GeomFromWkt(polygontable._c0)) as polygonshape from polygontable")
    // polygonDf.createOrReplaceTempView("polygondf")
    // polygonDf2.foreach(foreachFunc)
    polygonDf222.count()
    end = System.currentTimeMillis
    printf("Run Time (count)= %f[s]\n", (end - start) / 1000.0)

    start = System.currentTimeMillis
    //var polygonDf333 = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
    var polygonDf333 = sparkSession.sql("select ST_Centroid(ST_GeomFromWkt(polygontable._c0)) as polygonshape from polygontable")
    // polygonDf.createOrReplaceTempView("polygondf")
    polygonDf333.take(1)
    end = System.currentTimeMillis
    printf("Run Time (take(1))= %f[s]\n", (end - start) / 1000.0)

    start = System.currentTimeMillis
    //var polygonDf3353 = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
    var polygonDf3353 = sparkSession.sql("select ST_Centroid(ST_GeomFromWkt(polygontable._c0)) as polygonshape from polygontable")
    // polygonDf.createOrReplaceTempView("polygondf")
    polygonDf3353.take(100000)
    end = System.currentTimeMillis
    printf("Run Time (take(100000))= %f[s]\n", (end - start) / 1000.0)
//    var pointCsvDF = sparkSession.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
//    pointCsvDF.createOrReplaceTempView("pointtable")
//    pointCsvDF.show()
//    var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
//    pointDf.createOrReplaceTempView("pointdf")
//    pointDf.show()
    print(polygonCsvDf.count())
  }

  def testGeoSpark1():Unit =
  {
    val foreachFunc=(x:Any)=>1
    val geosparkConf = new GeoSparkConf(sparkSession.sparkContext.getConf)
    println(geosparkConf)

    var polygonCsvDf = sparkSession.read.format("csv").option("delimiter",",").option("header","false").load(csvPolygonInputLocation)
    polygonCsvDf.cache()
    polygonCsvDf.createOrReplaceTempView("polygontable")
    // polygonCsvDf.show()
    var start = 0.0
    var end = 0.0
    start = System.currentTimeMillis
    var polygonDf1 = sparkSession.sql("select ST_AsText(ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20)))) as polygonshape from polygontable")
    var polygonDf2 = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape_text from polygontable")
    // polygonDf.createOrReplaceTempView("polygondf")
    end = System.currentTimeMillis
    polygonDf1.show(3,0)
    polygonDf2.show(3,0)
    // polygonDf2.coalesce(1).write.csv("polygon.csv")
    printf("Run Time (collect)= %f[s]\n", (end - start) / 1000.0)

  }
}