//imports
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.ProcessingTime
import java.util.Date
import java.text.SimpleDateFormat;

object Main{

    def main(args: Array[String]) {
        val dateFormatter = new SimpleDateFormat("yyyy-MM-dd-hh-mm")
        val start_time = dateFormatter.format(new Date())

        val kafka_servers = sys.env("KAFKA_SERVERS")
        val kafka_topic = sys.env("KAFKA_TOPIC")
        val hdfs_server = sys.env("HDFS_SERVER")
        val hdfs_target_path = sys.env("HDFS_TARGET_PATH")
        val hdfs_ckpt_path = sys.env("HDFS_CKPT_PATH")
        val file_format = sys.env("FILE_FORMAT")

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
            .withColumn("year", from_unixtime($"epochtime"/1000,"yyyy"))
            .withColumn("month", from_unixtime($"epochtime"/1000,"MM"))
            .withColumn("day", from_unixtime($"epochtime"/1000,"dd"))
            .withColumn("hour", from_unixtime($"epochtime"/1000,"HH"))


        val query = clickstream
        .writeStream
        .format(file_format)
        .option("path", hdfs_server + "/" + hdfs_target_path)
        .option("checkpointLocation", hdfs_server + "/" + hdfs_ckpt_path)
        .partitionBy("year", "month", "day", "hour")
        .trigger(ProcessingTime("5 seconds"))
        .start()

        query.awaitTermination()

    }
}
