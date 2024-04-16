package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args){
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("MyApp");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("hdfs:///students/st50478/conrad-tajny-agent.txt"); // albo hdfs:///students/st50478/txt
        JavaRDD<String> words = lines.map(line -> line.split("[ ,;\\.]"))
                .flatMap(arr -> Arrays.asList(arr).iterator());
        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
        counts.foreach(el -> System.out.println(el));

    }
}