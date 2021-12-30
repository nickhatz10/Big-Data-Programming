# import statements that I will be using
# MRJob allows us to use mapreduce in a python environment
from mrjob.job import MRJob

# creates a class called LetterCount which has MRJob library passed through it
# this allows my class to have all the functions in the MRJob package plus whatever functions I define in the class
class LetterCount(MRJob):

    # mapper init function allows me to initialize certain variables, like my cache_letters dictionary
    def mapper_init(self):
        self.cache_letters = {}

    # mapper function takes a key and value input
    def mapper(self, key, line):

        # iterates through each specific character in the line
        for symbol in line:

            # if statement to make sure the character is a letter (in alphabet)
            if symbol.isalpha():

                # checks to see if the letter is already in our cache
                if symbol in self.cache_letters:
                    # if the letter is already in the cache then it will add 1 to that letters value
                    self.cache_letters[symbol] += 1
                    
                else:
                    # if the letter is not already in the cache_letters, then it will put the letter in the cache
                    # and give it a value of 1 since it was seen 1 time so far
                    self.cache_letters[symbol] = 1

                # checks to see if the cache_letters has more than 20 values in it
                if len(self.cache_letters) > 20:
                    # if it does have more than 20 values, then it will iterate through each key in the dictionary cache_letters
                    for letter in self.cache_letters:
                        # it will then use yield to talk to the reducer and give it the input of each letter in the dictionary
                        # followed by that specific letters cache_letters value, which is a count of how many times the letter appeared
                        yield(letter, self.cache_letters[letter])

                    # after doing this, the cache will then be cleared for the next values to be entered
                    # this way we do not constantly repeat the same values which would mess up the result
                    self.cache_letters.clear


    # this mapper final function acts as a cleanup function for any key-value pairs that are still sitting in the cache
    # and haven't been given to the reducer yet
    def mapper_final(self):
        # checks to see if the cache_letters is not empty
        if len(self.cache_letters) > 0:
            # if it isn't empty, then it will use the yield function to pass any values in the cache
            # to the reducer function. The reason we need this is because our cache_letters did not reach the
            # limit of 20 that we gave it in the mapper function and thus didn't send anything to the reducer
            # though this is because the document is done being parsed through so we need to send the remaining
            # key value pairs in the cache to the reducer
            for letter in self.cache_letters:
                yield (letter + "_", self.cache_letters[letter])

    def reducer_init(self):
        pass

    # reducer takes the inputs from the mapper function and sums up all of the values it was given
    # this will yield the total count of each letter in the document
    def reducer(self, key, values):
        yield (key, sum(values))

    def reducer_final(self):
        pass

# this allows our class LetterCount to run as the main program
if __name__ == '__main__':
    LetterCount.run()
