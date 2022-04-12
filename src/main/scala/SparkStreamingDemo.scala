import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.upper

object SparkStreamingDemo extends App {
  val path = if (args.length > 0) args(0)
  else "C:\\spark\\Spark-streaming\\files"

  import org.apache.spark.sql.types._

  val spark = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()

  val mySchema = StructType(
      StructField("id", LongType, nullable = false) ::
      StructField("city", StringType, nullable = false) ::
      StructField("country", StringType, nullable = false) :: Nil
  )

  val data = spark
    .readStream
    .format("csv")
    .option("header", value = true)
    .schema(mySchema)
    .load(path)

  val newFile = data
  .withColumn("upper_city", upper(data("city")))

  val output = newFile.writeStream
    .format("console")
    .start
  output.awaitTermination
}