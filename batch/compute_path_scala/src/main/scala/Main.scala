//imports
import scala.collection.mutable.{ListBuffer, HashMap}
import scala.math.{round, min, max, abs}

import org.apache.spark.Partitioner
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object Main{

    val usage = """
        Usage: target [ex:clickstreams-parquet/year=2018/month=02/day=13/hour=01]
    """

    def main(args: Array[String]) {
        if (args.length == 0) println(usage)

        val target = args(0)

        val hdfs_server = sys.env("HDFS_SERVER")
        val read_target = hdfs_server + "/" + target
        val write_target = hdfs_server + "/results/" + target

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

        val resolved_paths_df = user_agg_df.map(PathResolver.resolve).flatMap(e => e._2)
        val paths_rank_df = resolved_paths_df.rdd
        .map(path => (path, 1))
        .partitionBy(new PathPartitioner(30))
        .reduceByKey(_+_)
        .toDF("value","count")
        .sort(desc("count"))
        .limit(1000).write.json(write_target)

        //val paths_df = resolved_paths_df.selectExpr("CAST(value AS STRING)")
        //paths_df.createOrReplaceTempView("paths_rank")
        //val paths_rank_df = spark.sql("SELECT value,COUNT(*) as count FROM paths_rank GROUP BY value ORDER BY count DESC")

        paths_rank_df.limit(1000).write.json(write_target)

    }
}

class PathPartitioner(num_parts: Int) extends Partitioner {
    require(num_parts >= 1, s"Number of partitions ($num_parts) should be greater than 1.")
    //This is a map that contains information about the pathlength data distribution
    private val distribution_map: HashMap[Int,Double] = HashMap(
        (1, 0.30),
        (2, 0.21),
        (3, 0.15),
        (4, 0.10),
        (5, 0.07)
    )

    // Enforce a minimum of 10 partitions
    override def numPartitions: Int = max(10, num_parts)

    //This is a map with partitions bounds for requested number of partitions
    private val partition_set_map: HashMap[Int,(Int, Int)] = HashMap.empty[Int,(Int, Int)]
    private var part_lower_bound: Int = 0
    private var part_upper_bound: Int = 0

    for ((k, v) <- distribution_map) {
        part_upper_bound = part_lower_bound + round(v.floatValue * numPartitions) - 1
        partition_set_map += (k -> (part_lower_bound, part_upper_bound))
        part_lower_bound = part_upper_bound + 1
    }

    partition_set_map += (0 -> (part_upper_bound + 1, numPartitions - 1))


    override def getPartition(key: Any): Int =
        {
            var path_key = key.asInstanceOf[ListBuffer[Int]]
            val l = path_key.length
            var partition_set_key: Int = 0

            if (partition_set_map.contains(l)){
                partition_set_key = l
            } else {
                partition_set_key = 0
            }

            var lower = partition_set_map(partition_set_key)_1
            var upper = partition_set_map(partition_set_key)_2
            var out = lower + abs(path_key.hashCode()) % (upper - lower  + 1)
            out
        }

}
