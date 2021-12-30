# import statements that I will be using
# MRJob allows us to use mapreduce in a python environment
from mrjob.job import MRJob

# creates a class called NodeCount which has MRJob library passed through it
# this allows my class to have all the functions in the MRJob package plus whatever functions I define in the class
class NodeCount(MRJob):

    # mapper init function allows me to initialize certain variables, like my cache_node dictionary
    def mapper_init(self):
        self.cache_node = {}

    # mapper function takes a key and value input
    def mapper(self, key, line):
        # splits the line based on tabs
        nums = line.split("\t")
        # changes each value in the split list to an int using list comprehension
        nums = [int(x) for x in nums]
        # makes sure the second value in each list is greater than 1000
        # this is important since we want the neighbor id to be greater than 1000
        # if it is greater than 1000, it will check if the node id is in the cache
        # if it is in the cache then it will add 1 to its value
        # if not then it will create the key (node id) and its value (1 for 1 appearance)
        if nums[1] > 1000:
            if nums[0] in self.cache_node:
                self.cache_node[nums[0]] += 1

            else:
                self.cache_node[nums[0]] = 1
    	# checks to see if the cache_node has more than 50 values in it
        if len(self.cache_node) > 50:
            # if it does have more than 50 values, then it will iterate through each key in the dictionary cache_node
            for num in self.cache_node:
                # it will then use yield to talk to the reducer and give it the input of each node id in the dictionary
                # followed by that specific node id cache_node value, which is a count of how many times it appeared
                yield(num, self.cache_node[num])
    
            # after doing this, the cache will then be cleared for the next values to be entered
            # this way we do not constantly repeat the same values which would mess up the result
            self.cache_node.clear
    
    
    def mapper_final(self):

        # checks to see if the cache_node is not empty
        if len(self.cache_node) > 0:
            # if it isn't empty, then it will use the yield function to pass any values in the cache
            # to the reducer function. The reason we need this is because our cache_node did not reach the
            # limit of 50 that we gave it in the mapper function and thus didn't send anything to the reducer
            # though this is because the file is done being parsed through so we need to send the remaining
            # key value pairs in the cache to the reducer
            for node in self.cache_node:
                yield (node, self.cache_node[node])

    # initializes a new cache named sum_nums where we will store the sum of the values given to the reducer
    def reducer_init(self):
        self.sum_nums = {}

    # reducer takes the inputs from the mapper function and assings the key and sum of all the values
    # as a key, value pair in the sum_nums dictionary cache
    def reducer(self, key, values):
        
        self.sum_nums[key] = sum(values)

    # here, we will iterate through each key in the sum_nums dictionary and make sure that it's value is
    # greater than 5. If the value is greater than 5 then it will yield it which will give the output of
    # the node and the number of large neighbors it has (which would be greater than 5)
    def reducer_final(self):
        for num in self.sum_nums:
            if self.sum_nums[num] > 5:
                yield(num, self.sum_nums[num])     

# this allows our class NodeCount to run as the main program
if __name__ == '__main__':
    NodeCount.run()
