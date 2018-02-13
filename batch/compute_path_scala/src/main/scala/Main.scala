//imports
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

//import org.viirya.CountMinSketch._
import com.clearspring.analytics.stream.frequency.CountMinSketch
import java.util.Random

object Main{

    val usage = """
        Usage: target [ex:clickstreams-parquet/year=2018/month=02/day=13/hour=01]
    """

    def main(args: Array[String]) {
        if (args.length == 0) println(usage)

        val target = args(0)

        val hdfs_server = sys.env("HDFS_SERVER")
        val read_target = hdfs_server + "/" + target
        val write_target = hdfs_server + "/" + target + "-results"

        val spark = SparkSession
           .builder()
           .appName("Job that computes path rankings")
           .getOrCreate()

        // For implicit conversions like converting RDDs to DataFrames
        import spark.implicits._

        val df = spark.read.parquet(read_target).select(
            $"epochtime".cast(LongType),
            $"userid".cast(IntegerType),
            $"pageid_origin".cast(IntegerType),
            $"pageid_target".cast(IntegerType),
            $"case_status".cast(BooleanType)
            )

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

        val resolved_paths_df = user_agg_df.map(PathResolver.resolve).flatMap(e => e._2).toDF()
        val paths_df = resolved_paths_df.selectExpr("CAST(value AS STRING)")
        paths_df.createOrReplaceTempView("paths_rank")
        val paths_rank_df = spark.sql("SELECT value,COUNT(*) as count FROM paths_rank GROUP BY value ORDER BY count DESC")

        paths_rank_df.limit(1000).write.json(write_target)

    }
}
