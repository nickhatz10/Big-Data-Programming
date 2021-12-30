This is a program that I wrote for an assignment for my Data Science class. The assignment description is provided below to better understand the purpose of the program.

The Assignment. The homework consists of 3 files:
•hw1.py: This is where you will write your code (you can add functions if you want)
•driver.py: This file helps test your code (type python driver.py in the command line to run it)
•hwfunctions.py: This file contains functions your code needs to use.
The goal of this assignment is to create parallel versions of the following functions:

from hwfunctions import fun_inc, fun_factor

def serial_inc(start , end):
  return sum([ fun_inc(i) for i in range(start , end)])

def serial_factor(start , end):
  return sum([ fun_factor(i) for i in range(start , end)])


Your parallel versions must be significantly faster than the serial versions (your goal is to use the
ideas taught in class to see how much faster you can make it). After your parallel versions have been
added to hw1.py, you can type python driver.py. This will run your parallel code and compare it to the
serial code (it will compare answers and speed).

Question 1. In hw1.py, fill in the function delayed_increment(c, start, end). This function
must use dask delayed to create a parallel version of serial_inc. The first parameter c is the client, so
you do not need to create one yourself. The return value of the function must be a dask delayed object,
not the result of the computation (the code in driver.py will take the computation graph defined by your
delayed object and run it to get the answer). Your function must make use of the fun_inc function defined
in hwfunctions.py.

Question 2. In hw1.py, fill in the function delayed_factor(c, start, end). This function
must use dask delayed to create a parallel version of serial_factor. The first parameter c is the client, so
you do not need to create one yourself. The return value of the function must be a dask delayed object,
not the result of the computation (the code in driver.py will take the computation graph defined by your
delayed object and run it to get the answer). Your function must make use of the fun_factor function
defined in hwfunctions.py.


Question 3 In hw1.py, fill in the function future_increment(c, start, end). This function
must use the futures api to create a parallel version of serial_inc (this is like question 1, but using futures
instead of delayed). The first parameter c is the client, so you do not need to create one yourself. The return
value of the function must be a futures object, not the result of the computation (the code in driver.py
will obtain the answer from the futures object). Do not use delayed api in this function. Your function
must make use of the fun_inc function defined in hwfunctions.py.

Question 4. In hw1.py, fill in the function future_factor(c, start, end). This function must
use the futures api to create a parallel version of serial_factor. The first parameter c is the client, so you
do not need to create one yourself. The return value of the function must be a futures object, not the
result of the computation (the code in driver.py will obtain the answer from the futures object). Do not
use delayed api in this function. Your function must make use of the fun_factor function defined in
hwfunctions.py.
