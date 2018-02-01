//imports
import org.apache.spark.sql.SparkSession

import org.viirya.CountMinSketch._

object Main{
    def main(args: Array[String]) {

    val read_bucket_name = sys.env("READ_BUCKET_NAME")
    val write_bucket_name = sys.env("WRITE_BUCKET_NAME")
    val target_time = args(0)

    val spark = SparkSession
       .builder()
       .master("local[*]")
       .appName("Compute path app")
       .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val df = spark.read.json("s3a://" + read_bucket_name
        + "/clickstreams-" + target_time + "*")

    // Call API to calculate estimated frequencies of column "numbers"
    val results = countMinSketch(df, "userid").collect()

    results.foreach(println)

    }
}
