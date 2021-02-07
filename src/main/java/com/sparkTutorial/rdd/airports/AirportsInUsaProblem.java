package com.sparkTutorial.rdd.airports;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;

import java.text.Collator;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class AirportsInUsaProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
           and output the airport's name and the city's name to out/airports_in_usa.text.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:
           "Putnam County Airport", "Greencastle"
           "Dowagiac Municipal Airport", "Dowagiac"
           ...
         */
        Logger.getLogger("org").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("AirportInUSA").setMaster("local[*]");

        SparkContext sp = new SparkContext(conf);

        JavaRDD<String> input = sp.textFile("in/airports.text", 1 ).toJavaRDD();

        List<String> airportsInUSA = input.filter(line -> line.split(",")[3].contains("United States")).collect();

        System.out.println(airportsInUSA.size());
    }
}
