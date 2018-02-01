import org.viirya.CountMinSketch._
import org.apache.spark.sql.SparkSession

object Main{
    def main(args: Array[String]) {

    val spark = SparkSession.
    builder.master("local[*]")
     .appName("Simple Application")
    .getOrCreate()

    import spark.sqlContext.implicits._

    def toLetter(i: Int): String = (i + 97).toChar.toString

    // Generate a RDD
    val rows = Seq.tabulate(20) { i =>
      if (i % 3 == 0) (1, toLetter(1), -1.0) else (i, toLetter(i), i * -1.0)
      }

      // Create a DataFrame from the RDD
      val df = rows.toDF("numbers", "letters", "negDoubles")

      // Call API to calculate estimated frequencies of column "numbers"
      val results = countMinSketch(df, "numbers").collect()

      results.foreach(println)

    }
}
