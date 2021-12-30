# import statements that I will be using
# MRJob allows us to use mapreduce in a python environment
from mrjob.job import MRJob

# creates a class called WordLength which has MRJob library passed through it
# this allows my class to have all the functions in the MRJob package plus whatever functions I define in the class
class WordLength(MRJob):

    # mapper init function allows me to initialize certain variables, like my cache_word_length dictionary
    def mapper_init(self):
        self.cache_word_length = {}

    # mapper function takes a key and value input
    def mapper(self, key, line):
        
        # takes the input value which is a line of text/words and splits it into a list of each individual word
        words = line.split()

        # loops through each individual word in the list of words
        for word in words:

            # stores a variable named length which is the length of characters in each word
            length = len(word)

            # checks to see if the length value is already in our cache
            if length in self.cache_word_length:
                # if it is, then it will add the value 1 to whatever value is already with the key (length value of a word)
                self.cache_word_length[length] += 1
            else:
                # if the length value isn't already in the cache_word_length, then it will put that length value in it and give it a value of 1
                self.cache_word_length[length] = 1

            # checks to see if the cache_word_length has more than 20 values in it
            if len(self.cache_word_length) > 20:
                
                # if it does have more than 20 values, then it will iterate through each key in the dictionary cache_word_length
                for length in self.cache_word_length:
                    
                    # it will then use yield to talk to the reducer and give it the input of each length value in the dictionary
                    # followed by that specific 'length values' cache_word_length value, which is a count of how many times the 'length value' appeared
                    yield(length, self.cache_word_length[length])

                # after doing this, the cache will then be cleared for the next values to be entered
                # this way we do not constantly repeat the same values which would mess up the result
                self.cache_word_length.clear

    # this mapper final function acts as a cleanup function for any key-value pairs that are still sitting in the cache
    # and haven't been given to the reducer yet
    def mapper_final(self):
        
        # checks to see if the cache_word_length is not empty
        if len(self.cache_word_length) > 0:
            # if it isn't empty, then it will use the yield function to pass any values in the cache
            # to the reducer function. The reason we need this is because our cache_word_length did not reach the
            # limit of 20 that we gave it in the mapper function and thus didn't send anything to the reducer
            # though this is because the document is done being parsed through so we need to send the remaining
            # key value pairs in the cache to the reducer
            for length in self.cache_word_length:
                yield (length, self.cache_word_length[length])

    # reducer takes the inputs from the mapper function and sums up all of the values it was given
    # this will yield the total count of each 'length value' in the document
    def reducer(self, key, values):
        yield (key, sum(values))





# this allows our class WordLength to run as the main program
if __name__ == '__main__':
    WordLength.run()
