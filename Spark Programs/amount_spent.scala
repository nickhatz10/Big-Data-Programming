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
        val file = sc.textFile("hdfs:///ds410/retailclean")

        // uses map to split the file based on the tabs, since each column is seperated by tabs
        // we are splitting this csv based on each column value, we then get rid of the header
        // by filtering out the line that has the third index equal to the string Quantity
        val split_file = file.map(line => line.split("\t")).filter(x => x(3) != "Quantity")

        // uses map on the split file to make an rdd with the (customerID, totalAmountSpent)
        val id_amount = split_file.map(arr => (arr(6), (arr(3).toInt * arr(5).toFloat)))

        // we then take the (id, amount) RDD and apply aggregateByKey which will allow us to
        // count the number of times each customerID appears and also sum each customerID's total amount
        // this is important since it will allow us to get the average amount spent for each customerID
        val agg = id_amount.aggregateByKey((0,0.0))((num, value) => (num._1 + 1, num._2 + value),
        (num, total) => (num._1 + total._1, num._2 + total._2))

        // we then use map output create a tuple of (customerID, sum_totalAmount / custID_appearances)
        // in simple terms, this will be the (customerID, avgAmountSpent)
        val sum = agg.map{ case (x,y) => (x, (y._2 / y._1))}

        //we then save our output as a text file called q1 in our HDFS home directory
        sum.saveAsTextFile("q1")
    }
}
