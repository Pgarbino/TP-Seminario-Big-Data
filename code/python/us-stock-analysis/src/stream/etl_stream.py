import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    DoubleType,
    StructType,
    StructField,
    TimestampType,
)


def validate_params(args):
    if len(args) != 3:
        print(f"""
         |Usage: {args[0]} <brokers> <topics>
         |  <brokers> is a list of one or more Kafka brokers
         |  <topic> is a a kafka topic to consume from
         |
         |  {args[0]} kafka:9092 stocks
        """)
        sys.exit(1)
    pass


def create_spark_session():
    return (
        SparkSession
        .builder
        .appName("Stocks:Stream:ETL")
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "512m")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def start_stream(args):
    validate_params(args)
    _, brokers, topic = args

    spark = create_spark_session()

    json = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .load()
    )

    json.printSchema()

    # Explicitly set schema
    schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("price", DoubleType(), False),
    ])

    json_options = {"timestampFormat": "yyyy-MM-dd'T'HH:mm'Z'"}
    stocks_json = json.select(
        F.from_json(F.col("value").cast("string"), schema, json_options).alias("content")
    )

    stocks_json.printSchema

    stocks = stocks_json.select("content.*")

    #############################################################################
    # query1 | Parquet Output
    #############################################################################
    query1 = (
        stocks
        .withColumn("year", F.year(F.col("timestamp")))
        .withColumn("month", F.month(F.col("timestamp")))
        .withColumn("day", F.dayofmonth(F.col("timestamp")))
        .withColumn("hour", F.hour(F.col("timestamp")))
        .withColumn("minute", F.minute(F.col("timestamp")))
        .writeStream
        .format("parquet")
        .partitionBy("year", "month", "day", "hour", "minute")
        .option("startingOffsets", "earliest")
        .option("checkpointLocation", "/dataset/checkpoint")
        .option("path", "/dataset/streaming.parquet")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query1.awaitTermination(timeout=120)
    print("Stopping application.")
    print(
        "You can start a pyspark shell a run this:\n"
        "df = spark.read.parquet('/dataset/streaming.parquet')\n"
        "df.show()"
    )
    query1.stop()

    #############################################################################
    # query2 | Console Output | Average Price Aggregation
    #############################################################################
    # avg_pricing = (
    #     stocks
    #     .groupBy(F.col("symbol"))
    #     .agg(F.avg(F.col("price")).alias("avg_price"))
    # )

    # query2 = (
    #     avg_pricing
    #     .writeStream
    #     .outputMode("complete")
    #     .format("console")
    #     .trigger(processingTime="10 seconds")
    #     .start()
    # )

    # query2.awaitTermination()

    #############################################################################
    # query3 | Postgres Output | Simple insert
    #############################################################################
    # query3 = stream_to_postgres(stocks)
    # query3.awaitTermination()

    #############################################################################
    # query4 | Postgres Output | Average Price Aggregation
    #############################################################################
    # query4 = stream_aggregation_to_postgres(stocks)
    # query4.awaitTermination()

    #############################################################################
    # query5 | Postgres Output | Average Price Aggregation with Timestamp columns
    #############################################################################
    # query5 = stream_aggregation_to_postgres_final(stocks)
    # query5.awaitTermination()

    pass


def define_write_to_postgres(table_name):

    def write_to_postgres(df, epochId):
        print(f"Bacth (epochId): {epochId}")
        return (
            df.write
            .format("jdbc")
            .option("url", "jdbc:postgresql://postgres/workshop")
            .option("dbtable", f"workshop.{table_name}")
            .option("user", "workshop")
            .option("password", "w0rkzh0p")
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )
    return write_to_postgres


def stream_to_postgres(stocks, output_table="streaming_inserts"):
    wstocks = (
        stocks
        .withWatermark("timestamp", "60 seconds")
        .select("timestamp", "symbol", "price")
    )

    write_to_postgres_fn = define_write_to_postgres("streaming_inserts")

    query = (
        wstocks
        .writeStream
        .foreachBatch(write_to_postgres_fn)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    return query


def summarize_stocks(stocks):
    avg_pricing = (
        stocks
        .withWatermark("timestamp", "60 seconds")
        .groupBy(
            F.window("timestamp", "30 seconds"),
            stocks.symbol,
        )
        .agg(F.avg("price").alias('avg_price'))
    )
    avg_pricing.printSchema()
    return avg_pricing


def stream_aggregation_to_postgres(stocks, output_table="streaming_inserts_avg_price"):

    avg_pricing = summarize_stocks(stocks)

    window_to_string = F.udf(lambda w: str(w.start) + " - " + str(w.end), StringType())

    write_to_postgres_fn = define_write_to_postgres(output_table)

    query = (
        avg_pricing
        .withColumn("window", window_to_string("window"))
        .writeStream
        .foreachBatch(write_to_postgres_fn)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    return query


def stream_aggregation_to_postgres_final(stocks, output_table="streaming_inserts_avg_price_final"):

    avg_pricing = summarize_stocks(stocks)

    window_start_ts_fn = F.udf(lambda w: w.start, TimestampType())

    window_end_ts_fn = F.udf(lambda w: w.end, TimestampType())

    write_to_postgres_fn = define_write_to_postgres(output_table)

    query = (
        avg_pricing
        .withColumn("window_start", window_start_ts_fn("window"))
        .withColumn("window_end", window_end_ts_fn("window"))
        .drop("window")
        .writeStream
        .foreachBatch(write_to_postgres_fn)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    return query


if __name__ == "__main__":
    start_stream(sys.argv)
