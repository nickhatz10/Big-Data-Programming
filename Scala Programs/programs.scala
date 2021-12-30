case class Neumaier(sum: Double, c: Double)

object HW {
   //specifies that an Int will be returned
   //function checks to see if x is in the middle of the three numbers
   // and if it is returns x, if not it checks if y is in the middle of the 
   //three numbers and if it is, it returns y, and if neither x or y is in the middle
   // then it will return z
   def q1_middle(x: Int, y: Int, z:Int):Int = {
      if ((x > y & x < z) | (x < y & x > z))
      {
         x;
      }
      else if ((y > x & y < z) | (y < x & y > z))
      {
         y;
      }
      else
      {
         z;
      }
    
   }

   //specifies a String output
   //names a variable called name_lower which makes the name input to all lower case
   // if the inputs age is greater than or equal to 21, then it returns hello and then the lower case name
   // otherwise, it will return howdy and then the lowercase name
   def q2_interpolation(name: String, age: Int): String =  {
      
      val name_lower = name.toLowerCase()
      if (age >= 21) 
      {
          s"hello, $name_lower";
      }
      else
      {
          s"howdy, $name_lower";
      }
      
   }


   // specifies a Double return type
   // uses the foldLeft function to input a tuple, with the first 0.0 represnting the sum
   // and the second 0 representing the index
   // the foldLeft will iterate through each value in the array and add them
   // together while also performing other operations
   // after each value it will also add 1 to the index, it returns a double of the sum of the array
   def q3_polynomial(arr: Seq[Double]):Double = {
     
      val sum = arr.foldLeft((0.0, 0)){(x, y) => (x._1 + y*scala.math.pow(2, x._2), x._2 + 1)};
      sum._1;
   }
   
   // uses the function to input x and y, and then assigns the output to a variable one
   // uses the function again to input y and z, and assigns the ouput to a variable two
   // then uses the function to input one and two and returns this as the output, which is an Integer
   def q4_application(x: Int, y: Int, z: Int)(f: (Int, Int) => Int): Int = {
      
      val one = f(x,y);
      val two = f(y,z);
      f(one, two);
   }

   // specifies the return as a vector string
   // uses the tabulate function to add n which is the number of iterations to the start
   // this will allow us to have a vector whos starting point is a value greater than 0
   // it then converts each Int in the vector to a String, and returns the vector string
   def q5_stringy(start: Int, n: Int): Vector[String] = {
  
      val vec_nums =  Vector.tabulate(n){n => (n + start)}
      vec_nums.map(_.toString)


   }

   //specifies a vector int return type
   // filters based on x being greater than or equal to the input a
   // then filters again based on none of the values in the vector being a multiple of input b
   // returns the filtered vector
   def q6_modab(a: Int, b: Int, c:Vector[Int]): Vector[Int] = {

      val filt_c =  c.filter(x => x >= a);
      filt_c.filter(x => x % b != 0);


   }

   // specifies a return type of Integer
   // uses the map function to create key value pairs based on the index of each value as the key
   // and the boolean value produced from the function as the values
   // then uses the toMap to make an immutable dictionary
   //next it filters the dictionary to only choose values that have boolean true as their values
   // it then checks to see if the dictionary is empty, and if it is return -1
   // otherwise it will take a list of the keys(indexes) in the dictionary and return the max value as an Int
   def q7_find(vec: Vector[Int])(f: (Int) => Boolean): Int = {

      val bool_vec = vec.map{x =>(vec.indexOf(x), f(x))}.toMap
      val bool_true = bool_vec.filter{case (k,v) => v == true} 
      if (bool_true.isEmpty == true){
	 -1;
      }  
      else {
         bool_true.keys.max;
      }
   }

   //@annotation.tailrec
   def q8_find_tail(vec: Vector[Int])(f: (Int) => Boolean): Int = {

      
      val bool_vec = vec.map{x =>(vec.indexOf(x), f(x))}.toMap
      val bool_true = bool_vec.filter{case (k,v) => v == true}
      if (bool_true.isEmpty == true){
         -1;
      }
      else {
         bool_true.keys.max;
      }
   }

   def q9_neumaier(seq_doub: Seq[Double]): Double = {

      0.0
      
   }


}
