import org.apache.spark.sql.SparkSession

import scala.::

object SparkStreamingDemo extends App {
  val path = "C:\\spark\\Spark-streaming\\files"
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

  val spark = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()


  val mySchema = StructType(
    StructField("id", LongType, nullable = false) ::
    StructField("city", StringType, nullable = false) ::
    StructField("country", StringType, nullable = false) :: Nil )

  val data = spark
    .readStream
    .format("csv")
    .option("header",true)
    .schema(mySchema)
    .load(path)


  val output = data.writeStream
    .format("console")
    .start
    output.awaitTermination
}

//  val newFile = data
//    .withColumn("upper_city", upper(data("city")))