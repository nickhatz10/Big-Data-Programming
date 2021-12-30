This is a scala program that I wrote for an assignment for my Data Science class. The assignment description is provided below to better understand the purpose of the program.

Question 1. In the HW object, fill in the function q1 middle. It is supposed to take in 3 integers x,y,z
and return the middle value in sorted order. For example, if the input is 5, 1, 4, the output should be 4.
You cannot use lists, vectors or any other collections. No loops or recursion either. Don’t use the return
keyword. You can use if statements and you can define helper functions (anonymous functions or other
non-recursive functions). You must update the function definition to specify its return type.

Question 2. In the HW object, fill in the q2 interpolation function so that it returns (not prints) a greeting
message (do not use the return keyword). The inputs to the function are name and age. If the person is 21
or over, it should greet with “hello” and otherwise it should greet with “howdy”. It should always lowercase
the name (remember, scala strings are basically java strings). For example, q2 interpolation(“Carmen”, 23)
should return the string “hello, carmen” and q2 interpolation(“Carmen”, 20) should return “howdy, carmen”.
You must use string interpolation. You must update the function definition to specify its return type. You
are encouraged to try do this question in one line.

Question 3. In the HW object, fix the q3 polynomial function. Its input is any sequence type that con-
tains Doubles (i.e., it works with List[Double], Vector[Double], Array[Double], etc. because they are all
Seq[Double]). Its output is supposed to be ∑n−1
i=0 input(i) ∗2i, where n is the number of elements in the
input sequence arr (note in Scala that indexing uses parentheses instead of square brackets). For example
q3 polynomial(List(2.0, 3.0, 4.0)) should return 2.0 ∗1 + 3.0 ∗2 + 4.0 ∗4 = 24.0. Use only foldLeft (no indexing
and no other list/vector operations) to solve this problem. If you are stuck, think about how to do this in
python. In python you would create a loop:
# maybe do something here
for x in arr:
# definitely do something here
figure out what state you need to maintain (maybe the state is a tuple) and how you update that state.
Then use this information to use foldLeft correctly. Do not use the return keyword. You must update the
function definition to specify its return type.

Question 4. In the HW object, fill in the body of q4 application. It takes three integers x, y, z and a function
f (f takes two Ints and returns an Int). Your function should return f (f (x, y), f (y, z)). Look in tester.scala
to see how we call this function. Do not use the return keyword. You must update the function definition
to specify its return type.

Question 5. In the HW object, create a function q5 stringy(start, n) whose inputs are both integers. It
should return a Vector of the first n integers starting from start as Strings. For example q5 stringy(2, 3)
should return Vector(“2”, “3”, “4”). You must use the tabulate function. Do not use the return keyword.
You must update the function definition to specify its return type.

Question 6. In the HW object, create a function q6 modab(a, b, c) where a and b are Ints and c is a Vector
of Ints. It should return a vector of elements of c that are greater than or equal to a and are not multiples of
b. For example, q6 modab(2, 3, V ector(1, 2, 3, 4, 5, 6, 7, 8, 9)) should return Vector(2,4,5,7,8). You may only
use maps and filters. Do not use the return keyword. You must update the function definition to specify its
return type.

Question 7. In HW object, write a function q7 f ind that takes an input (of type Vector[Int]) and a function
f (from Ints to Booleans) and returns the index of the last element for which f returns true (-1 if there is no
such element). We should be able to call this function as q8_find(Vector(1,3,4,5,6,7,8)){x => x%2==0}
(in this case, it should return 6, because the element at position 6 is 8, which is even). Do not use the return
keyword. You must update the function definition to specify its return type.

Question 8. In HW object, write a function q8 f ind tail that does the same thing as q7 f ind. However
q8 f ind tail must be tail recursive and you must use @annotation.tailrec to get scala to check that it
really is tail recursive. Also, you are not allowed to use indexing, but you are allowed to use head and tail.
For an x that has a sequence type (e.g., List or Vector), x.head returns the first element and x.tail returns
the rest. For example, if x=Vector(1,2,3) then x.head is 1 and x.tail is Vector(2,3). If you are stuck, consider
how you would do this in python using a while loop and then convert it into a tail recursive function as
discussed in class.
