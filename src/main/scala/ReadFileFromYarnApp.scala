object ReadFileFromYarnApp extends App {
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("ReadFileFromHadoopApp")
    .master("yarn")
    .getOrCreate()

  val df = spark
    .read
    .option("header", true)
    .option("inferSchema", true)
    .csv("hdfs://localhost:9000/user/damian/stocks.csv")

  df.show()
}
