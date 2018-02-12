//imports
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.ProcessingTime

object Main{

    def main(args: Array[String]) {
        val kafka_servers = sys.env("KAFKA_SERVERS")
        val kafka_topic = sys.env("KAFKA_TOPIC")
        val bucket_name = sys.env("BUCKET_NAME")

        val spark = SparkSession
           .builder()
           .appName("Job that persists clickstream data from Kafka to CSV files on S3")
           .getOrCreate()

        // For implicit conversions like converting RDDs to DataFrames
        import spark.implicits._

        val kafka_stream = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_servers)
            .option("subscribe", kafka_topic)
            .load()

        val clickstream = kafka_stream
            .select(
                get_json_object(($"value").cast("string"), "$.epochtime").alias("epochtime"),
                get_json_object(($"value").cast("string"), "$.userid").alias("userid"),
                get_json_object(($"value").cast("string"), "$.pageid_origin").alias("pageid_origin"),
                get_json_object(($"value").cast("string"), "$.pageid_target").alias("pageid_target"),
                get_json_object(($"value").cast("string"), "$.case_status").alias("case_status")
            )


        val query = clickstream
        .writeStream
        .format("console")
        .outputMode("append")
        .trigger(ProcessingTime("5 seconds"))
        .start()

        query.awaitTermination()

    }
}
