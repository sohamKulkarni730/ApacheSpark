package com.soham.sparkPractice;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class salesDataAnalysis
{

    public static void main(String[] args)
    {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder().appName("Sales data analysis").master("local[*]").getOrCreate();

        Dataset<Row> salesData=spark.read().option("header","true").csv("D:\\DataSets\\sales\\2m Sales Records.csv");

        String[] columns=   salesData.columns() ;

        Arrays.stream(columns).forEach( v -> {
                                                System.out.println(v);
                                                salesData.select(v).distinct().show();
                                                System.out.println("\n\n\n");
                                             });

    }
}
