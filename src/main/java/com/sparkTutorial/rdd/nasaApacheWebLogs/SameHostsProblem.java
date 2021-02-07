package com.sparkTutorial.rdd.nasaApacheWebLogs;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;

public class SameHostsProblem {

    public static void main(String[] args) throws Exception {

        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
           Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

           Example output:
           vagrant.vf.mmc.com
           www-a1.proxy.aol.com
           .....

           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Make sure the head lines are removed in the resulting RDD.
         */

        SparkConf conf = new SparkConf().setAppName("SameHostProblem").setMaster("local[2]") ;

        SparkContext sp = new SparkContext(conf) ;

        JavaRDD<String> julyFile = sp.textFile("in/nasa_19950701.tsv" ,1).toJavaRDD();
        JavaRDD<String> augFile = sp.textFile("in/nasa_19950801.tsv",1).toJavaRDD();



        julyFile = julyFile.map(line -> line.split("\t")[0]) ;
        augFile = augFile.map(line -> line.split("\t")[0]) ;

        JavaRDD<String> aggregated = julyFile.intersection(augFile) ;

        JavaRDD<String> cleanLogLines = aggregated.filter(line -> isNotHeader(line));

        aggregated.saveAsTextFile("out/finalOutput.tsv");


    }
    private static boolean isNotHeader(String line) {
        return !(line.startsWith("host") || line.contains("bytes"));
    }
}
