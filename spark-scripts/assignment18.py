import pyspark
import os
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql.functions import window, expr


dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

spark_hostname = os.getenv("SPARK_MASTER_HOST_NAME")
spark_port = os.getenv("SPARK_MASTER_PORT")
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

# definisikan path untuk folder checkpoint karena menggunakan stateful transformation
checkpoint_path = "/spark-scripts/checkpoint"

spark_host = f"spark://{spark_hostname}:{spark_port}"

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 org.postgresql:postgresql:42.2.18"

sparkcontext = pyspark.SparkContext.getOrCreate(
    conf=(pyspark.SparkConf().setAppName("DibimbingStreaming").setMaster(spark_host))
)
sparkcontext.setLogLevel("WARN")
spark = pyspark.sql.SparkSession.builder.appName("DibimbingStreaming").getOrCreate()

stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{kafka_host}:9092")
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .load()
)

# parsing json yang dihasilkan producer menjadi string untuk mempermudah data manipulation
parsed_stream_df = stream_df.selectExpr("CAST(value AS STRING)").selectExpr("from_json(value, 'ts TIMESTAMP, price INT') as data").select("data.*")

# membuat batas waktu maksimal data terlambat yang masih bisa diproses, karena casenya daily total purchase maka batas maksimum nya adalah 1 hari
windowed_stream_df = parsed_stream_df.withWatermark("ts", "1 day").groupBy(window("ts", "1 day")).agg(expr("sum(price) as total_purchase"))

# Menampilkan kolom "Timestamp" and "Running total" dengan tipe output complete mode agar semua agregasi pada setiap batch bisa dijumlahkan
(
    windowed_stream_df
    .selectExpr("window.start as Timestamp", "total_purchase as Running_total")
    .writeStream
    .outputMode("complete") 
    .format("console")
    .option("checkpointLocation", checkpoint_path)
    .start()
    .awaitTermination()
)


