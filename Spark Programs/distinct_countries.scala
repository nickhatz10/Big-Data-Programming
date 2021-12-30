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

        // stores the retailclean csv file from hdfs as an RDD named file
        val file = sc.textFile("hdfs:///ds410/retailclean")

        // uses map to split the file based on the tabs, since each column is seperated by tabs
        // we are splitting this csv based on each column value, we then get rid of the header
        // by filtering out the line that has the third index equal to the string Quantity
        val split_file = file.map(line => line.split("\t")).filter(x => x(3) != "Quantity")

        // next, we use map to create a tuple that stores the (customerID, country)
        // we apply thr distinct function to this tuple to get rid of any duplicates from the tuple
        // this is necessary since we only want to count the total number of different/distinct countries
        // that a customer purchased from
        val dist = split_file.map(arr => (arr(6), arr(7))).distinct()

        // we then use the map function to isolate only the customerID's from the tuple
        val custID = dist.map(arr => arr._1)

        // next we use map again to take each customerID that we have and assigns it to a tuple (customerID, 1)
        // this allows us to later count the total number of times each customerID appears, which would
        // give us the total number of distinct countries each customer purchased from
        val mapped = custID.map(custID => (custID, 1))

        // we then use reduceByKey to add up each value of each customerID which gives us the total
        // appearances of each customerID, or in other words number of distinct countries for each purchase
        val count = mapped.reduceByKey(_+_)

        //we then save our output as a text file called q2 in our HDFS home directory
        count.saveAsTextFile("q2")
    }
}
