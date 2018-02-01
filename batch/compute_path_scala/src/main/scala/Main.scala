//imports
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

import org.viirya.CountMinSketch._

object Main{
    def main(args: Array[String]) {

    val spark = SparkSession
       .builder()
       .master("local[*]")
       .appName("Compute path app")
       .getOrCreate()

    //import spark.sqlContext.implicits._
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val df = spark.read.json("/home/robin/Documents/insight/dev/insight-project/local/sample-data.json")

    // Call API to calculate estimated frequencies of column "numbers"
    val results = countMinSketch(df, "userid").collect()

    results.foreach(println)

    }
}
