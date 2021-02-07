package com.sparkTutorial;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PluralsightCourseReader {

    public static void main(String[] args)  {

        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("PluralSight Course Reader").setMaster("local[1]");

        JavaSparkContext spark = new JavaSparkContext(conf);

        JavaRDD<String> courseLines = spark.textFile("in/Courses.csv");



//creating a stopword DISTRIBUTED corpus

        /*
        JavaRDD<String> stopwords_file =  spark.textFile("in/stopwords_en.txt") ;
        // created a distributed dataset for stopwords.
        // This is working now as we are doing spark execution locally , and ram is shared between both workers .
        // But will not work on distributed set up
        // We need a single collection of stopwords in all of the spark-workers
        // how to achieve that ???
        // May be using   broadcast
        Map<String, Long> stopwords =  stopwords_file.flatMap(value -> Arrays.stream(value.split("\n")).iterator())
                .mapToPair(word -> new Tuple2 (word, 1L))
                .collectAsMap();
         */

// creating a stopword LOCAL corpus
        Stream<String> stopwords = null ;
        try {
            stopwords = Files.lines(Paths.get("in/stopwords_en.txt")) ;
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<String> stopwords_list = stopwords.flatMap(lines -> Arrays.stream(lines.split("\n"))).collect(Collectors.toList());
        Set<String> stopwords_set = new HashSet<String>();
        stopwords_set.addAll(stopwords_list) ;


//
        System.out.println("##########      Partitions are  " + courseLines.getNumPartitions() + "    ############## \n\n\n");

// NON Fluent API version - Very verbose but easy to read

        /*courseLines.takeSample(false, 1)
                   .stream()
                   .flatMap(line -> Arrays.stream(line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")))
                   .forEach(m -> System.out.println( m + "\n\n" ));*/

       /* JavaRDD<String> description_words = courseLines.flatMap( line -> Arrays.stream(line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")[4]
                .split("\\s|\\-"))
                //.filter(v -> ! stopwords.containsKey(v))
                .iterator());


        JavaPairRDD<String,Long> description_word_count =  description_words.mapToPair( value -> new Tuple2<String, Long>( value, 1L)) ;

        JavaPairRDD<String,Long> description_word_count2 =  description_word_count.reduceByKey( (x,y)-> x + y) ;

        JavaPairRDD <Long, String> count_and_word_description = description_word_count2.mapToPair( tuple -> new Tuple2<Long, String>( tuple._2, tuple._1));

        JavaPairRDD<Long, String> sorterd_count_and_word_description =  count_and_word_description.sortByKey(false);

        sorterd_count_and_word_description.take(50).forEach(System.out::println);
*/

//fluent API version , very compact but difficult to read --  performance compared - does not affect the performance

        courseLines.flatMap( line-> Arrays.stream(line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")[4]
                                          .split("\\s|\\-"))
                                          .iterator())
                   .filter( key_word -> !stopwords_set.contains(key_word))
                   .mapToPair( word -> new Tuple2<String,Long>(word, 1L))
                   .reduceByKey( (word_count1, word_count2)  -> word_count1 + word_count2)
                   .mapToPair(word_tuple -> new Tuple2<Long, String>(word_tuple._2, word_tuple._1))

                   .sortByKey(false)
                   .take(50)
                   .forEach(System.out::println);
    }

}
