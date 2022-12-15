package BSES.LateAggregation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}

object Consumer_BSES extends App {
  val spark: SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("SparkByExample")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "BSES")
    .option("startingOffsets", "latest") // From starting

    .load()

  df.printSchema()

  val schema = new StructType()
    .add("ad_id", StringType)
    .add("tag", StringType)
    .add("time", TimestampType)


  val df2 = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")

  val df3 = df2.select(from_json(col("value"), schema).as("sample")
    , col("timestamp"))

  val df4 = df3.select("sample.*", "timestamp")
  df4.printSchema()

  df4.writeStream.format("console").start()

  val df5 = df4.select("ad_id","tag","time", "timestamp")

  print(df5.printSchema())

  val bi_Signal_SES = new Bi_Signal_SES(10,1,60*60,df5)
  bi_Signal_SES.run()
  spark.streams.awaitAnyTermination()



}
