//imports
import org.apache.spark.sql.SparkSession

import org.viirya.CountMinSketch._

object Main{

    val usage = """
        Usage: --env [local|aws] --target [ex:20180130T125955-1517319]
    """

    def main(args: Array[String]) {
        if (args.length == 0) println(usage)

        var current_environment = ""
        var target = ""
        args.sliding(2, 2).toList.collect {
          case Array("--env", argEnv: String) => current_environment = argEnv
          case Array("--target", argTarget: String) => target = argTarget
        }

        if (current_environment == "aws"){
            val spark_master = sys.env("SPARK_MASTER")
            val read_bucket_name = sys.env("READ_BUCKET_NAME")
            val write_bucket_name = sys.env("WRITE_BUCKET_NAME")
            read_target = "s3a://" + read_bucket_name + "/clickstreams-" + target_time + "*"
        } else if (current_environment == "local") {
            val spark_master = "local[*]"
            var read_target = ""/home/robin/Documents/insight/dev/insight-project/local/sample-data.json"
        } else {
            println(usage)
        }


        val spark = SparkSession
           .builder()
           .master(spark_master)
           .appName("Compute path app")
           .getOrCreate()

        // For implicit conversions like converting RDDs to DataFrames
        import spark.implicits._

        val df = spark.read.json(read_target)

        // Call API to calculate estimated frequencies of column "numbers"
        val results = countMinSketch(df, "userid").collect()

        results.foreach(println)

    }
}
