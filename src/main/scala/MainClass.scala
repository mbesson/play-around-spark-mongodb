import com.mongodb.spark._
import com.mongodb.spark.config._
import com.mongodb.spark.rdd.MongoRDD
import org.bson.Document
import org.apache.spark.sql.SparkSession


/**
  * Created by mathieu on 26/02/2017.
  */
object MainClass extends App{
  println ("Connecting Spark with MongoDB")

  val spark = SparkSession.builder()
                          .master("local")
                          .appName("MongoSparkConnectorIntro")
                          .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/nasa.eva")
                          .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/nasa.astronautTotals")
                          .getOrCreate()

  spark.catalog.listDatabases()

}
