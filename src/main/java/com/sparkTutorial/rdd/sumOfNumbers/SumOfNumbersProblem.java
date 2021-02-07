package com.sparkTutorial.rdd.sumOfNumbers;

import com.google.inject.internal.cglib.core.$CollectionUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class SumOfNumbersProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
           print the sum of those numbers to console.

           Each row of the input file contains 10 prime numbers separated by spaces.


         */
        Logger.getLogger("org").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("Sum Of Prime Numbers").setMaster("local[*]") ;

        SparkContext sp = new SparkContext(conf) ;

        JavaRDD<String> primeNumbers = sp.textFile("in/prime_nums.text", 1).toJavaRDD();  

        JavaRDD<String> numbers = primeNumbers.flatMap(i -> Arrays.asList(i.split("\\s")).iterator());

        JavaRDD<String> cleanNumbers = numbers.filter(i -> !i.isEmpty());

        JavaRDD<Integer> IntegerNumber = cleanNumbers.map(i-> Integer.valueOf(i));

        int sum = IntegerNumber.reduce(Integer::sum);

        System.out.println(sum);





        //JavaRDD numbersInInt = numbers.map(i -> Integer.valueOf(i.toString())) ;


    }


}
