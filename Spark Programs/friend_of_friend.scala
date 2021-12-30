//generic import statements

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

//code must be inside an object
object HW6 {

    // entry
    def main(args: Array[String]) = {

	// create a configuration
        val conf = new SparkConf().setAppName("wc")

	// create the sc
        val sc = new SparkContext(conf)

	// stores the facebook file from hdfs as an RDD titled fb
        val fb = sc.textFile("hdfs:///ds410/facebook")

	// uses map to split the RDD based on tabs, which makes every row an array of strings
        val split_file = fb.map(line => line.split("\t"))

	// uses map to make every row an array of tuples of two ints
        val kvrdd = split_file.map(arr => (arr(0).toInt, arr(1).toInt))

	// creates a reversed value that is kvrdd in reverse
	val kvrdd_reverse = kvrdd.map(x => (x._2, x._1))

	// joins the original and the reversed kvrdd 
	val kv_joined = kvrdd.join(kvrdd_reverse)

	// uses map to make an rdd of only the values of the kv_joined tuple
	// it then uses filter to get rid of any values that have the same numbers
	// and then uses distinct to make sure there are no repeating values
	val only_values = kv_joined.map(x => x._2).filter(x => x._1 != x._2).distinct()

	// then we use aggregateByKey which allows us to store the count of each specific
	// nodes neighbors, followed by the sum of each specific nodes neighbors of neighbors
	val agg = only_values.aggregateByKey((0,0))((num, value) => (num._1 + 1, num._2 + value),
        (num, total) => (num._1 + total._1, num._2 + total._2))

	// then I use map to only choose the specific node followed by the sum of that nodes
	// neighbors of neighbors
	val answer = agg.map{ case (x,y) => (x, y._2)}

	//we then save our output as a text file called q3 in our HDFS home directory
        answer.saveAsTextFile("q3")
    }
}
