package com.soham.sparkPractice;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.* ;

// Progress
/*
14/2/2021  - Spark able to read JSON and schema correctly - No Analytics
14/2/2021  - Ran dome EDA (Exploratory data analysis) found data to be too messy to get anything from it .
             Will focus on a well pre-processed data first and come back to this data set for any pattern analysis

*/

public class CSODReportingAnalysis {



/*  // CSOD has Reporting API that can given report of Training data, Transcript data and various other data related to Learning and development
    // The analytics exercise ifs aimed at finding any pattern and carrying some analytics over the data
    // 1 .Data usually comes in Json format and has to be parsed to Java Objects -> data set or dataframe  or
    //    Dictionary (preferred in python ). that is first task
          // 1.1 .  JSON Data needs to be in JSON-newline format  i.e. every JSON object has to be in separate line and separated by new line
          //         Also a valid Json object has keys in string format with double quotes.
          //         check  these  before sending to schema validation
          // 1.2 .  Json parser can work even if JSON-Newline format is not present -use multiline to true , in this case spark has to read all the data first ,
          //        this is time consuming
          // 1.3    Providing schema to JSON will always be faster and is a must in case of Spark-Streaming application

    // 2. Once we have data in structured format - SQL like commands can perform analytics  to describe the data superficially
    // 3. Once data is understood we can perform statistical analysis to describe the data in statistical terms
    // 4. Lastly we build any hypothesis -> est the hypothesis -> make machine learning model on that data
*/

    public static void main(String[] args) throws IOException {

        Logger.getLogger("org").setLevel(Level.ERROR);

        Stream<String> training_data = null;

        if (!Files.exists(Paths.get("C:\\Java_WorkSpace_JB\\Spark\\sparkTutorial\\in\\csod_reporting_table_views\\training_view_formatted.json")))
// JSON reader and Formatter to JSON-NewLine Format
        // Read Multiline JSON file
        {
            System.out.println("Creating Formmated Data file ");

            training_data = Files.lines(Paths.get("C:\\Java_WorkSpace_JB\\Spark\\sparkTutorial\\in\\csod_reporting_table_views\\training_view.json"));
            //Dont Use String- Use string Buffer / String Builder -> mutable object thus less overhead
            final StringBuffer s = training_data.collect(StringBuffer::new, (x, y) -> x.append(y), (a, b) -> a.append(b));
            //Get the actual data in value tag
            JSONArray training_data1 = new JSONObject(s.toString()).getJSONArray("value");
            // convert it to list of JSON object converted to a single string -- one JSON - one string - one line
            List<String> training_data2 = new ArrayList<>();

            for (Object i : training_data1) {

                training_data2.add(i.toString());

            }

            Files.write(Paths.get("C:\\Java_WorkSpace_JB\\Spark\\sparkTutorial\\in\\csod_reporting_table_views\\training_view_formatted.json"), training_data2);
            training_data.close();
        }

// Spark Starts the activity
        SparkConf conf = new SparkConf().setAppName("CSOD Reporting Analysis").setMaster("local[*]");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        Dataset<Row> training_records1 = spark.read().option("multiline", "true")
                                              .json("C:\\Java_WorkSpace_JB\\Spark\\sparkTutorial\\in\\" +
                                                      "csod_reporting_table_views\\training_view_formatted.json");
// Step 1 :: Print schema and check whether dta is being read properly .
        //training_records1.printSchema();

        String[] columns = training_records1.columns() ;

        training_records1.describe();
// Always close the session ... A must
        spark.close();
    }


}
