package MSES
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}



object Consumer_MSES extends App{

  val spark: SparkSession = SparkSession
      .builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()


  spark.sql("SET spark.sql.streaming.metricsEnabled=true")
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "MSES")
      .option("startingOffsets", "latest") // From starting
      
      .load()

    df.printSchema()

    val schema = new StructType()
      .add("ad_id", StringType)
      .add("start_time", TimestampType)
      .add("end_time", TimestampType)


    val df2 = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")

    val df3 = df2.select(from_json(col("value"), schema).as("sample")
      , col("timestamp"))

    val df4 = df3.select("sample.*", "timestamp")
    df4.printSchema()

    df4.writeStream.format("console").start()

    val df5 = df4.select("start_time","end_time","timestamp")

    print(df5.printSchema())



    val mono_Signal_SES =  new Mono_Signal_SES(5,1, 10,df5)
    mono_Signal_SES.run()

    spark.streams.awaitAnyTermination()


}
