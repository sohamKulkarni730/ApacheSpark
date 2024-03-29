package com.soham.sparkPractice;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple5;
import scala.reflect.ClassTag;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.*;

public class whstAppAnalysis_Dataframe
{

    public static void main(String[] args)
    {
        Logger.getLogger("org").setLevel(Level.ERROR);
// Capture the System time in nano seconds : this gives us very rough estimate of time required for a step completion
        Long t = System.currentTimeMillis();

        SparkConf conf = new SparkConf().setAppName("whatsapp data analysis").setMaster("local[*]");

      /*  JavaSparkContext spark_ctx = new JavaSparkContext(conf);

        Configuration hadoopConfig = spark_ctx.hadoopConfiguration();
        hadoopConfig.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        hadoopConfig.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());*/


        SparkSession spark_ss = SparkSession.builder().config(conf).getOrCreate();

        spark_ss.conf().set("spark.sql.shuffle.partitions", 3);

        JavaSparkContext spark_cxt = JavaSparkContext.fromSparkContext(spark_ss.sparkContext()) ;

        //Arrays.stream(spark_cxt.getConf().getAll()).forEach(v -> System.out.println(v._1 + " "+  v._2));

        Broadcast<Set<String>> stopwords_set = spark_cxt.broadcast(get_StopWords_set()) ;

        spark_ss.udf().register("wordCount_in_message_withoutStopWords", (String Message)->  Message.split(" ").length
                                                                                      ,DataTypes.IntegerType);

        spark_ss.udf().register("wordCount_message_withStopWords", (String Message)->  Arrays.stream(Message.split(" "))
                        .filter(v -> !stopwords_set.value().contains(v))
                        .collect(Collectors.toList())
                        .size()
                ,DataTypes.IntegerType);

        String base_directory = "C:\\Java_WorkSpace_JB\\Spark\\whatsapp_chat" ;

        String chat_name = "WhatsApp_Chat_AB_SK.txt" ;

        System.out.println(base_directory +"\\in\\"+chat_name);

        Dataset<Row> chat = spark_ss.read().textFile(base_directory +"\\in\\"+chat_name)
                //.where(col("value").isNotNull())
                .select( to_date(regexp_extract(col("value"), "^[0-9\\/]*",0 ), "dd/MM/yyyy").alias("Date"),
                        regexp_extract(col("value"), "(?<=,\\s)(.*?)(?=\\s-)",0 ).alias("timestamp"),
                        regexp_extract(col("value"), "(?<=\\-\\s)(.*?)(?=\\:)",0 ).alias("Actor"),
                        regexp_extract(col("value"), "(?<=\\:\\s)(.*)",0 ).alias("Message")
                )
                .withColumn("words_withStopWords", callUDF("wordCount_message_withStopWords", col("Message")))
                .withColumn("words_withoutStopWords", callUDF("wordCount_in_message_withoutStopWords", col("Message")))
                ;




              /*+----------+---------+--------------+--------------------+-----+
                |      Date|timestamp|         Actor|             Message|words|
                +----------+---------+--------------+--------------------+-----+
                |2019-11-04|    01:29|              |                    |    1|
                |2019-11-04|    01:29|Soham Kulkarni|Hi amruta , this ...|    7|
                |2019-11-04|    01:29|Soham Kulkarni|Sorry i missed ur...|    6|
                |2019-11-04|    01:29|  Amruta Bonde|Ohh so this is yo...|    7|
                |2019-11-04|    01:29|Soham Kulkarni|  I v reached saftly|    4|
                +----------+---------+--------------+--------------------+-----+
              */

        //chat.cache();
//reduceBykey trial
        SQLContext sqlContext = spark_ss.sqlContext();

        JavaPairRDD chat_a = chat.javaRDD().mapToPair(row -> new Tuple2<>(row.getString(2),  1L ))
                                           .reduceByKey((a,b) -> a+b) ;

        chat_a.take(5).stream().forEach(System.out::println);

        JavaRDD chat_b = JavaPairRDD.toRDD(chat_a).toJavaRDD();

        chat_b.take(5).stream().forEach(System.out::println);



        /*Dataset<Row> chat_a = sqlContext.createDataFrame(JavaPairRDD.toRDD(chat.javaRDD().mapToPair(row -> new Tuple2<>(row.getString(2),  1L ))
                .reduceByKey((a,b) -> a+b)) , Encoders.tuple(Encoders.STRING(),Encoders.LONG())) ;
*/

         chat.groupBy(col("Actor")).count().show();
               /* +--------------+-----+
                |         Actor|count|
                +--------------+-----+
                |  Amruta Bonde| 8525|
                |Soham Kulkarni| 7440|
                |              |  205|
                +--------------+-----+*/

         chat.groupBy(col("Date"), col("Actor")).count().sort(to_date(col("Date"), "dd/MM/yyyy")).show();
               /* +----------+--------------+-----+
                |      Date|         Actor|count|
                +----------+--------------+-----+
                |04/11/2019|              |    1|
                |04/11/2019|  Amruta Bonde|   51|
                |04/11/2019|Soham Kulkarni|   41|
                |05/11/2019|Soham Kulkarni|   21|
                |05/11/2019|  Amruta Bonde|   27|*/

//PIVOT 1::
         chat.groupBy(col("Date")).pivot( col("Actor")).count().sort(to_date(col("Date"), "dd/MM/yyyy")).show();
               /*+----------+----+------------+--------------+
                 |      Date|    |Amruta Bonde|Soham Kulkarni|
                 +----------+----+------------+--------------+
                 |04/11/2019|   1|          51|            41|
                 |05/11/2019|null|          27|            21|
                 |06/11/2019|null|          39|            33|
                 |07/11/2019|null|          38|            31|
                 |08/11/2019|null|          65|            55|
        */

//PIVOT 2:: CHAT1
        // To reduce burden on pivot operations . pivot operation run an sql query on column of interest
        List <Object> actors_values = chat.select(col("Actor")).distinct().collectAsList().stream()
                /* Following Map operation may look unnecessary as we are returning list of object
                   However, while we pass it to pivot function, Catalyst optimiser receives a Raw Object
                   and not string.
                   This causes unsupported-literal-exception*/
                .map(m -> m.get(0).toString())
                .collect(Collectors.toList());

        Dataset<Row> chat1 = chat.select(col("words_withStopWords"),col("Date"),col("Actor"))
                .groupBy(col("Date"))
                .pivot(col("Actor"), actors_values)
                .count()
                .sort(col("Date"))
                .withColumn("Day", date_format(col("Date"), "E"))
                .drop(col(""))
                .na().drop();

        chat1.show();
// Multiple aggregations ::
        chat1.groupBy(col("Day")).agg(sum("Amruta Bonde").alias("Amruta Bonde"),
                                               sum("Soham Kulkarni").alias("Soham Kulkarni"),
                                               count("Date").alias("Days")
                                               )
                                          .show();
           /*
           +---+------------+--------------+----+
            |Day|Amruta Bonde|Soham Kulkarni|Days|
            +---+------------+--------------+----+
            |Sun|        1482|          1360|  63|
            |Mon|        1420|          1267|  67|
            |Thu|        1020|           957|  67|
            |Sat|        1099|           879|  66|
            |Wed|        1104|           943|  66|
            |Fri|        1253|          1149|  66|
            |Tue|        1107|           885|  66|
            +---+------------+--------------+----+*/


//WORDS ::CHAT2
        Dataset<Row> chat2 = chat.select(col("words_withStopWords"),col("Date"),col("Actor"))
                .groupBy(col("Date"),col("Actor"))
                .agg(sum("words_withStopWords").alias("words"))
                .sort(col("Date"))
                .na().drop();

        chat2.show();

               /* +----------+--------------+----------+
                |      Date|         Actor|sum(words)|
                +----------+--------------+----------+
                |2019-11-04|Soham Kulkarni|       270|
                |2019-11-04|  Amruta Bonde|       246|
                |2019-11-04|              |         1|
                |2019-11-05|  Amruta Bonde|       101|
                |2019-11-05|Soham Kulkarni|       134|
                |2019-11-06|  Amruta Bonde|       141|*/

//WORDS ::CHAT3

        Dataset<Row> chat3 = chat.withColumn( "Day", date_format(col("Date"), "E"))
                .groupBy(col("Day"))
                //providing values in label save time as spark does not have to run a unique query to receive all possible value
                .pivot(col("Actor"), Arrays.asList(new String[]{"Soham Kulkarni","Amruta Bonde"}))
                .agg(sum("words_withoutStopWords"))
                .drop(col(""))
                .sort(col("Day").cast(DataTypes.DateType))
                .na().drop();
        chat3.show();

        Dataset<Row> chat4 = chat.withColumn( "month", date_format(col("Date"), "MMMM"))
                .groupBy(col("month"))
                .pivot(col("Actor"), Arrays.asList(new String[]{"Soham Kulkarni","Amruta Bonde"}))
                .agg(sum("words_withoutStopWords"))
                .drop(col(""))
                .sort(col("month").cast(DataTypes.DateType))
                .na().drop();
        chat4.show();

        Dataset<Row> chat5 = chat.withColumn( "month_year", date_format(col("Date"), "MM/yyyy"))
                .groupBy(col("month_year"))
                .pivot(col("Actor"), Arrays.asList(new String[]{"Soham Kulkarni","Amruta Bonde"}))
                .agg(sum("words_withoutStopWords"))
                .drop(col(""))
                .sort(col("month_year"))
                .na().drop();
         chat5.show();


        System.out.println("##############################\n\n\n\t\ttime to execute :"+ (System.currentTimeMillis() - t) +"\n\n\n##############################");
              /*Scanner in = new Scanner(System.in) ;
        in.nextLine() ;*/

    }

    public static List<String> chat_regex_ListString(String input_msg)
    {
        // Date (?<=^)(.*?)(?=\,)
        Pattern date = Pattern.compile("^[0-9\\/]*");
        // timestamp (?<=,\s)(.*?)(?=\s\-)
        Pattern timestamp = Pattern.compile("(?<=,\\s)(.*?)(?=\\s-)");
        // Actor (?<=\-\s)(.*?)(?=\:)
        Pattern Actor = Pattern.compile("(?<=\\-\\s)(.*?)(?=\\:)");
        // Message in text  (?<=\:\s)(.*)
        Pattern Message = Pattern.compile("(?<=\\:\\s)(.*)");

        Matcher m_timestamp = timestamp.matcher(input_msg);
        Matcher m_date = date.matcher(input_msg);
        Matcher m_Actor = Actor.matcher(input_msg);
        Matcher m_Message = Message.matcher(input_msg);

        List<String> result = new ArrayList<String>();

        if (m_date.find() & m_timestamp.find() & m_Actor.find() & m_Message.find()) {
            result.add(m_date.group(0));
            result.add(m_timestamp.group(0));
            result.add(m_Actor.group(0));
            result.add(m_Message.group(0));
        }

        return result;

    }

    public static Set<String> get_StopWords_set ()
    {
        Stream<String> stopwords = null;
        try {
            stopwords = Files.lines(Paths.get("C:\\Java_WorkSpace_JB\\Spark\\sparkTutorial\\in\\stopwords_en.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<String> stopwords_list = stopwords.flatMap(lines -> Arrays.stream(lines.split("\n"))).collect(Collectors.toList());
        Set<String> stopwords_set = new HashSet<String>();
        stopwords_set.addAll(stopwords_list);

        return stopwords_set ;
    }
}
