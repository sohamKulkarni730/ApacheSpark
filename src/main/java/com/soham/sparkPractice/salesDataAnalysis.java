package com.soham.sparkPractice;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

import java.util.Arrays;
import java.util.Scanner;

public class salesDataAnalysis {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder().appName("Sales data analysis").master("local[*]").getOrCreate();

        Dataset<Row> salesData = spark.read().option("header", "true").csv("D:\\DataSets\\sales\\2m Sales Records.csv");

        String[] columns = salesData.columns();
        // 0-Region,1-Country,2-Item Type,3-Sales Channel,4-Order Priority,5-Order Date,6-Order ID,7-Ship Date,8-Units Sold
        //9-Unit Price,10-Unit Cost,11-Total Revenue,12-Total Cost,13-Total Profit

        // Describe the data type - Nominal, Ordinal, Discrete, Continuous
        // Group by operation can be done on Ordinal , nominal data
        //
       /* Arrays.stream(columns).forEach( v -> {
                                                System.out.println(v);
                                               *//* salesData.select(v).distinct().show();
                                                System.out.println("\n\n");*//*
                                             });*/

        // caching improves performance
        Dataset<Row> newdata = salesData.select(columns[2], columns[1], columns[13]);

        Dataset<Row> newdata1 = newdata.groupBy(columns[1])
                                       .agg(first(columns[2]), sum((columns[13])))
                                       .filter(col("first(Item Type)").equalTo("Household"))
                                       .orderBy("sum(Total Profit)");


        Dataset<Row> newdata2 = newdata.groupBy(columns[1]).count();
               // .agg( max(col(columns[1]).cast(DataTypes.LongType)));

        Dataset<Row> newdata3 = salesData.groupBy(columns[1]).pivot(columns[2]).count();

        Dataset<Row> newdata4 = salesData.groupBy(columns[1],columns[2])
                                        .count()
                                        .agg((count(columns[2])));



        newdata.show();
        newdata1.show();
        newdata2.show();
        newdata3.show();
        newdata4.show();



        /*Scanner in = new Scanner(System.in) ;
        in.nextLine() ;*/
    }

}
