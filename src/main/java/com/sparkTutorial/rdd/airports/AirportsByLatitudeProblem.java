package com.sparkTutorial.rdd.airports;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.*;
import org.apache.spark.sql.*;

import java.util.List ;
import java.util.stream.IntStream;
import static org.apache.spark.sql.functions.* ;

public class AirportsByLatitudeProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
           Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:
           "St Anthony", 51.391944
           "Tofino", 49.082222
           ...
         */
         Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("Airport with latitude more than 40 ").setMaster("local[*]") ;

        SparkSession spark_ss =  SparkSession.builder().config(conf).getOrCreate();

        Dataset<Row> airports = spark_ss.read().csv("in/airports.text" ) ;


        String[] new_columns = {"Airport ID", "Name of airport", "Main city served by airport", "Country where airport is located", "IATA/FAA code",
               "ICAO Code", "Latitude", "Longitude", "Altitude", "Timezone", "DST", "Timezone in Olson format"} ;

        airports = airports.toDF(new_columns);

        Dataset<Row>airports1 = airports.select(col("Name of airport"), col("Longitude"))
                .where(col("Longitude").$greater$eq(40));

         airports1.show(10);
    }
}
