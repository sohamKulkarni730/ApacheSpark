package com.soham.sparkPractice;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.*;
import scala.Tuple2;
import scala.Tuple4;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


public class whatsAppAnalysis {

    public static void main(String[] args) throws IOException {

        Logger.getLogger("org").setLevel(Level.ERROR);
// Capture the System time in nano seconds : this gives us very rough estimate of time required for a step completion 
        Long t = System.currentTimeMillis();

        SparkConf conf = new SparkConf().setAppName("whatsapp data analysis").setMaster("local[*]");

        JavaSparkContext spark_ctx = new JavaSparkContext(conf);

        Configuration hadoopConfig = spark_ctx.hadoopConfiguration();
        hadoopConfig.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        hadoopConfig.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());


        SparkSession spark_ss = SparkSession.builder().config(conf).getOrCreate();


        String base_directory = "C:\\Java_WorkSpace_JB\\Spark\\whatsapp_chat" ;

        String chat_name = args[0] ;

        System.out.println(base_directory +"\\in\\"+chat_name);

        Set<String> stopwords = get_StopWords_set() ;


        JavaRDD<Tuple4> chats = spark_ctx.textFile(base_directory +"\\in\\"+chat_name)
                .map(v -> chat_regex_ListString(v))
                .filter(chat -> chat.size() != 0)
                .map(string ->
                        new Tuple4<String, String, String, String>(string.get(0),
                                string.get(1),
                                string.get(2),
                                string.get(3).replaceAll("[^a-zA-Z0-9\\s]","").toLowerCase())

                );
        List<String> word_pair_count = chats.flatMap(v -> IntStream.range(1, v._4().toString().split(" ").length)
                                                                    //v._4() would always return Object and not string withing mapToObject method
                                                                   .mapToObj(i -> v._4().toString().split(" ")[i-1]
                                                                                        .concat(" " + v._4().toString().split(" ")[i]))
                                                                   .collect(Collectors.toList()).stream().iterator())
                .mapToPair(v1 -> new Tuple2<String, Long>(v1, 1L))
                .reduceByKey((v2, v3) -> v2 + v3)
                .mapToPair(v4 -> new Tuple2<Long, String>(v4._2(), v4._1()))
                .filter(w2 -> w2._1()>1L)
                .sortByKey(false)
                .map(v5 -> v5._2().concat("," + v5._1().toString()))
                .collect();


        Path out = Paths.get(base_directory+"/out/word_count/"+chat_name+"_word_count.txt");
        Path out_csv = Paths.get(base_directory+"/out/chat_to_csv/"+chat_name+".csv");

        List<String> chat_list = null;

      /* if(!Files.exists(out_csv))
          {
              chat_list = chats.map(v -> v.toString().replaceAll("[()]", "")).collect() ;

              Files.write(out_csv, chat_list , Charset.forName("ISO-8859-1"));
          }*/


        chat_list = chats.map(v -> v.toString().replaceAll("[()]", "")).collect() ;

        Files.write(out_csv, chat_list , Charset.forName("UTF-8"));

        Files.write(out, word_pair_count , Charset.forName("UTF-8"));

        System.out.println(System.currentTimeMillis() - t);



    }

// creating a stopword LOCAL corpus
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

// Function to convert whatsapp chat to csv string
// Whatspp chat history always comes in following format :
// 07/11/2019, 11:07 - Soham Kulkarni: Kk
// date, HH:MM - actor: chat message string
    public static String chat_regex_CSVString(String input_msg) {
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

        StringBuilder result = new StringBuilder("");

        if (m_date.find())
            result.append(m_date.group(0) + ",");
        if (m_timestamp.find())
            result.append(m_timestamp.group(0) + ",");
        if (m_Actor.find())
            result.append(m_Actor.group(0) + ",");
        if (m_Message.find())
            result.append(m_Message.group(0) + ",");

        return result.toString();

    }

    public static List<String> chat_regex_ListString(String input_msg) {
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

    public static List<String> sentence_word_list(String input) {
        String[] words = input.split(" ");

        List<String> result = IntStream.range(1, words.length).mapToObj(i -> words[i-1].concat(" " + words[i])).collect(Collectors.toList());
        List<String> result2 = IntStream.range(2, words.length).mapToObj(i -> words[i-2].concat(" " + words[i-1] +" "+ words[i])).collect(Collectors.toList());
        //System.out.println(result.size());
        result.addAll(result2) ;
        return result;
     }


}

