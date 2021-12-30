//generic import statements

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

//code must be inside an object
object MR_Spark {

    // entry
    def main(args: Array[String]) = {

	// create a configuration
        val conf = new SparkConf().setAppName("wc")

	// create the sc
        val sc = new SparkContext(conf)

        // stores the retailclean file from hdfs as an RDD titled file
        val file = sc.textFile("hdfs:///ds410/facebook")

        // uses map to split the file based on the tabs, since the values in each row are seperated by tabs
        val split_file = file.map(line => line.split("\t"))
        // turns the string values in each row to ints and then filters them to make sure the second value > 1000
        val kvrdd = split_file.map(arr => (arr(0).toInt, arr(1).toInt)).filter(x => x._2 > 1000)
        // once filtered, we only want the first values or in other terms, the node values
        val nodes = kvrdd.map(arr => arr._1)        
        // creates a key value pair of the node id and the value 1
        val kv_pairs = nodes.map(node => (node, 1))
        // sums up all the times the node id appears
        val count = kv_pairs.reduceByKey(_ + _)
        // if the node id doesn't appear more than 5 times then it is disregarded
        val filt_count = count.filter(x => x._2.toInt > 5)
        // saves the output as a text file to HDFS called q2_output
        kvrdd.saveAsTextFile("q2_output")


    }
} 
