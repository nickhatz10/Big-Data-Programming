# program written by Nick Hatzenbeller

# import the functions that we will be using from the hwfunctions file
# import dask delayed as well to run the delayed parallel functions
from hwfunctions import fun_factor, fun_inc
from dask import delayed

# uses a delayed decorator to wrap the helper_inc function
# this allows us to not have to add delayed in front of the function when we call it later
# this is a helper function which means we will be calling this functions multiple times in our main function below
# this allows us to save time by consolidating code. We will not have to rewrite
# all of this code multiple times in our main function
@delayed
def helper_inc(start, end):
    # uses list comprehension to iterate through numbers in the range from start to end
    # applies the fun_inc function to all the values in this range
    lst = [(fun_inc)(i) for i in range(start, end)]

    # after applying the fun_inc function to all numbers in the range
    # we then sum all of the numbers in the output from the list comprehension
    sum_1 = sum(lst)

    # returns this sum
    return sum_1

# uses a delayed decorator to wrap the helper_fac function
# this allows us to not have to add delayed in front of the function when we call it later
# this is a helper function which means we will be calling this functions multiple times in our main function below
# this allows us to save time by consolidating code. We will not have to rewrite
# all of this code multiple times in our main function
@delayed
def helper_fac(start, end):
    # uses list comprehension to iterate through numbers in the range from start to end
    # applies the fun_factor function to all the values in this range
    lst = [(fun_factor)(i) for i in range(start, end)]

    # after applying the fun_factor function to all numbers in the range
    # we then sum all of the numbers in the output from the list comprehension
    sum_1 = sum(lst)

    # returns this sum
    return sum_1

# we do not need a decorator here since this function will be using futures and not delayed
# this is a helper function which means we will be calling this functions multiple times in our main function below
# this allows us to save time by consolidating code. We will not have to rewrite
# all of this code multiple times in our main function
def helper_inc2(start, end):
    # uses list comprehension to iterate through numbers in the range from start to end
    # applies the fun_inc function to all the values in this range
    lst = [(fun_inc)(i) for i in range(start, end)]

    # after applying the fun_inc function to all numbers in the range
    # we then sum all of the numbers in the output from the list comprehension
    sum_1 = sum(lst)

    # returns this sum
    return sum_1

# we do not need a decorator here since this function will be using futures and not delayed
# this is a helper function which means we will be calling this functions multiple times in our main function below
# this allows us to save time by consolidating code. We will not have to rewrite
# all of this code multiple times in our main function
def helper_fac2(start, end):
    # uses list comprehension to iterate through numbers in the range from start to end
    # applies the fun_factor function to all the values in this range
    lst = [(fun_factor)(i) for i in range(start, end)]

    # after applying the fun_factor function to all numbers in the range
    # we then sum all of the numbers in the output from the list comprehension
    sum_1 = sum(lst)

    # returns this sum
    return sum_1

# this functions takes c as an input which is simply starting the Client which will allow us to work in parallel
# it also takes start and end which will be integers
def delayed_increment(c, start, end):

    # subtracts the end from the start and assigns it to a variable named diff
    diff = end - start

    # creates a list named operations
    # uses the helper_inc function to split the input into four different lists of the same length
    # and then the helper_inc function will return the sum of these four individual lists
    # this is useful since it will allow us to work more quickly in parallel
    # instead of the worker nodes having to communicate millions of times (once per iteration of the for loop in fun_inc)...
    # they will only communicate 4 times since the helper function is wrapped in the delayed
    # instead of the fun_inc function itself being wrapped
    operations = [helper_inc(start, start + diff // 4), helper_inc(start + diff // 4, start + diff // 2),
                  helper_inc(start + diff // 2, start + int(diff // (4 / 3))),
                  helper_inc(start + int(diff // (4 / 3)), end)]

    # once the worker nodes are done splitting up input into four different lists of the same length
    # they will use delayed to take the sum of all of the operation sums in parallel
    summed = delayed(sum)(operations)

    # this will return the dask delayed sum object
    return summed

# this functions takes c as an input which is simply starting the Client which will allow us to work in parallel
# it also takes start and end which will be integers
def delayed_factor(c, start, end):

    # subtracts the end from the start and assigns it to a variable named diff
    diff = end-start

    # creates a list named operations
    # uses the helper_fac function to split the input into four different lists of the same length
    # and then the helper_fac function will return the sum of these four individual lists
    # this is useful since it will allow us to work more quickly in parallel
    # instead of the worker nodes having to communicate millions of times (once per iteration of the for loop in fun_factor)...
    # they will only communicate 4 times since the helper function is wrapped in the delayed
    # instead of the fun_factor function itself being wrapped
    operations = [helper_fac(start, start + diff // 4), helper_fac(start + diff // 4, start + diff // 2),
                  helper_fac(start + diff // 2, start + int(diff // (4 / 3))),
                  helper_fac(start + int(diff // (4 / 3)), end)]

    # once the worker nodes are done splitting up input into four different lists of the same length
    # they will use delayed to take the sum of all of the operation sums in parallel
    factor_sum = delayed(sum)(operations)
    return factor_sum



# this is basically the same concept as the delayed_increment function except this uses a different technique
# this uses futures to work in parallel so instead of using delayed it will use c.submit() to tell
# the Client it wants to work in parallel, which allows the code to be executed more quickly than the serial version
# it again takes three parameters, c calls the Client, start and end is the range to be looped through
def future_increment(c, start, end):

    # subtracts the end from the start and assigns it to a variable named diff
    diff = end - start

    # creates a list named operations which uses the helper function to split this list into
    # four different lists of the same length, the futures submit is then used to run each helper function in parallel
    # this way the incrementing done in the helper function happens in parallel and is much quicker
    # the helper function then returns four sums of the increments stored in the list
    operations = [c.submit(helper_inc2, start, start + diff // 4), c.submit(helper_inc2,start + diff // 4, start + diff // 2),
                  c.submit(helper_inc2, start + diff // 2, start + int(diff // (4 / 3))),
                  c.submit(helper_inc2, start + int(diff // (4 / 3)), end)]

    # futures is used again to sum the four sums in the operations list together
    summed_sum = c.submit(sum, operations)

    # a futures object of the total sum is returned
    return summed_sum


# this is basically the same concept as the delayed_factor function except this uses a different technique
# this uses futures to work in parallel so instead of using delayed it will use c.submit() to tell
# the Client it wants to work in parallel, which allows the code to be executed more quickly than the serial version
# it again takes three parameters, c calls the Client, start and end is the range to be looped through
def future_factor(c, start, end):

    # subtracts the end from the start and assigns it to a variable named diff
    diff = end - start

    # creates a list named operations which uses the helper function to split this list into
    # four different lists of the same length, the futures submit is then used to run each helper function in parallel
    # this way the computations done in the helper function happens in parallel and is much quicker
    # the helper function then returns four sums of the computations stored in the list
    operations = [c.submit(helper_fac2, start, start + diff // 4),
                  c.submit(helper_fac2, start + diff // 4, start + diff // 2),
                  c.submit(helper_fac2, start + diff // 2, start + int(diff // (4 / 3))),
                  c.submit(helper_fac2, start + int(diff // (4 / 3)), end)]

    # futures is used again to sum the four sums in the operations list together
    summed_sum = c.submit(sum, operations)

    # a futures object of the total sum is returned
    return summed_sum
