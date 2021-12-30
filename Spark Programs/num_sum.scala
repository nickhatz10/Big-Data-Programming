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

        // stores the retailclean file from hdfs as an RDD titled file
        val file = sc.textFile("hdfs:///ds410/facebook")

        // uses map to split the file based on the tabs, since each node and neighbor is seperated by tabs
        val split_file = file.map(line => line.split("\t"))
        
        // splits split_file into (int, int)
        val kvrdd = split_file.map(arr => (arr(0).toInt, arr(1).toInt))
        // only chooses values where the second value is greater than the first
        val greater_than = kvrdd.filter(arr => arr._1 < arr._2)
        // chooses the first value which is the node from kvrdd
        val nodes = kvrdd.map(arr => arr._1)
        // chooses first value which is node from greater than
        val great_than_nodes = greater_than.map(arr => arr._1)
        // assigns the 1 value as the key to each node in nodes
        val kv_pairs = nodes.map(node => (node, 1))
        // assigns 1 value as the key to each node in great_than_nodes 
        val kv_great_than = great_than_nodes.map(node => (node, 1))
        // adds up all of the appearances of each node
        val count = kv_pairs.reduceByKey(_ + _)
        // adds up the total sum of each nodes neighbor id
        val sum_counts = kvrdd.reduceByKey(_ + _)
        // adds up all the times a node's neighbor id is greater than it
        val great_than_count = kv_great_than.reduceByKey(_ + _)
        // joins together all the rdd's to create an rdd with 
        //(node, appearances, sum, greater than neighbor appearances)
        val final_array_join = count.join(sum_counts).join(great_than_count)
  
        // saves the final file as a text file called q3_output to HDFS
        final_array_join.saveAsTextFile("q3_output")


    }
} 
