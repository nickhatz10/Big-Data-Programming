These are some Spark programs that I wrote in Scala for different assignments for my Data Science class. The assignment description for each seperate Spark assignment is provided below to better understand the purpose of the program.


AMOUNT SPENT. Write spark code to do the following. Using the /ds410/retailclean dataset,
for each CustomerID, compute the average amount spent per order. Remember that a customer can make
multiple purchases and the average is total spent divided by total number of orders (hint: do not use joins).
Save the output in the file “q1” in hdfs (i.e., saveAsTextFile(“q1”)). If you do this correctly, a line in the
output should look like (CustomerID, AverageAmountSpent) (e.g., (12345, 4.50)).


DISTINCT COUNTRIES. Write spark code to do the following. Using the /ds410/retailclean dataset,
for each CustomerID, compute the number of distinct countries they made a purchase from. Save the output
in the file “q2” in hdfs. You may find it helpful to use the scala Set (see the cheatsheet, scala book, or scala
documentation).


FRIEND OF FRIEND. Write spark code to do the following. Using the /ds410/facebook dataset,
for each node, compute the sum of the ids of the (distinct) friend-of-friends it has. Save the output in the
file “q3” in hdfs. This question involves, among other things, a join. For example, if the data looks like this:
1 2
1 3
2 1
3 1
3 5
2 4
2 5
2 3
3 2
4 2
5 2
5 3
Then 1 is friends with 2 and 2 is friends with 3,4,5, so 3,4,5 are friends of friends of 1. However, 1 is also
friends with 3 and 3 is friends with 2,5, so 2 (and 5) is also a friend-of-friend of 1 (1 is not a friend-of-friend
of itself because no node is allowed to be a friend-of-friend of itself). Hence 1 has 4 distinct friend-of-friends
(which are 2,3,4,5, and their sum is 14) and so (1, 14) should definitely appear in the output file.
Also note that the facebook dataset is symmetric/redundant. That is, if an edge (a,b) appears in it then
so will (b,a).


LARGE NEIGHBORS. In the facebook dataset (/ds410/facebook), every line is a pair of
numbers separated by tabs. The first number is the “node” id and the second is its “neighbor” id. A neighbor
is “large” if its id is bigger than 1000. For each node, we are interested in the number of large neighbors
it has, but only if it has at least 5 large neighbors. So in the output, each line would be a node and the
number of large neighbors it has. Nodes with less than 5 large neighbors should not be in the output. Write
a Spark program that does this.


NUM_SUM. In this question, we are again using the Facebook dataset. For each
node X, we want to count (1) the number of neighbors of X, (2) the sum of the ids of the neighbors of X,
(3) the number of neighbors with ids larger than X. For example, if the input is
3 4
3 2
2 1
the output would look something like:
3 (2, 6, 1)
2 (1, 1, 0)
Solve this problem using Spark RDDs.
