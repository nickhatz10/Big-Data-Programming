

WORD LENGTH. We want to do a word-size count on “War and Peace” (/ds410/warandpeace in HDFS). That
is, for every number, we want to know how many words have that many characters in it. In the output, the
key will be the word length and the value will be the number of words with that length.
For example, if the input file is:
always use git from
the command line!
the output would be (your lines might appear in a different order):
6 1
3 3
4 1
5 1
7 1
For example, “lines!” has 5 characters (so we have the line 5 1) and there are 3 words with 3 char-
acters (so we have the line 3 3). Use line.split() to split a line into words. Words can have
punctuations in them, so “the” and “the,” are two different words.



LETTER COUNT. The lettercount.py code counts the number of times each letter appears. It is inefficient because
of all of the messages that the mapper is sending. Use in-memory combining (also known as in-mapper
combining) to make lettercount more efficient.



LARGE NEIGHBORS. In the facebook dataset (/ds410/facebook), every line is a pair of
numbers separated by tabs. The first number is the “node” id and the second is its “neighbor” id. A neighbor
is “large” if its id is bigger than 1000. For each node, we are interested in the number of large neighbors
it has, but only if it has at least 5 large neighbors. So in the output, each line would be a node and the
number of large neighbors it has. Nodes with less than 5 large neighbors should not be in the output. Write
a mapreduce program that does this.
