//imports
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

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

        var spark_master = ""
        var read_bucket_name = ""
        var write_bucket_name = ""
        var read_target = ""

        if (current_environment == "aws"){
            spark_master = sys.env("SPARK_MASTER")
            read_bucket_name = sys.env("READ_BUCKET_NAME")
            write_bucket_name = sys.env("WRITE_BUCKET_NAME")
            read_target = "s3a://" + read_bucket_name + "/clickstreams-" + target + "*"
        } else if (current_environment == "local") {
            spark_master = "local[*]"
            read_target = target
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

        val df = spark.read.json(read_target).select(
            $"epochtime".cast(LongType),
            $"userid".cast(IntegerType),
            $"pageid_origin".cast(IntegerType),
            $"pageid_target".cast(IntegerType),
            $"case_status".cast(BooleanType)
            )
        df.describe().show()

        // Order by userid (primary) and epochtime (secondary)
        val ordered_df = df.orderBy("userid", "epochtime")

        // Aggregate records per userid
        val epochtime_list = collect_list($"epochtime").alias("epochtimes_list")
        val pageid_origin_list = collect_list($"pageid_origin").alias("pageid_origin_list")
        val pageid_target_list = collect_list($"pageid_target").alias("pageid_target_list")
        val case_status_list = collect_list($"case_status").alias("case_status_list")

        val user_agg_df =  ordered_df
          .groupBy($"userid")
          .agg(epochtime_list, pageid_origin_list, pageid_target_list, case_status_list)

        user_agg_df.show()

        object PathResolver {
            def resolve(user_records: Row): (Int, ListBuffer[ListBuffer[Int]]) = {
                val userid = user_records.getAs[Int]("userid")
                val pageid_origin_list = user_records.getAs[Seq[Int]]("pageid_origin_list")
                val pageid_target_list = user_records.getAs[Seq[Int]]("pageid_target_list")
                val case_status_list = user_records.getAs[Seq[Boolean]]("case_status_list")


                val paths_buff = ListBuffer[ListBuffer[Int]]()
                for((hop, count) <- pageid_target_list.zipWithIndex){

                    if(paths_buff.isEmpty) {
                        paths_buff += ListBuffer[Int](hop)
                    } else {
                        paths_buff.last += hop
                    }

                    if(case_status_list(count)){paths_buff += ListBuffer[Int]()}
                }
                (userid, paths_buff)
            }
        }

        val paths_df = user_agg_df.map(PathResolver.resolve).flatMap(e => e._2).toDF()

        // Call API to apply Count Min Sketch on paths
        val results = countMinSketch(paths_df, "value").orderBy(desc("value_freq")).show()
        //results.foreach(println)

    }
}
