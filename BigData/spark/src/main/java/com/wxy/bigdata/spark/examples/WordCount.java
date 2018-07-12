package com.wxy.bigdata.spark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCount {
    public static void main(String[] args) {
        String inputFile = args[0],outputFile = args[1];
        SparkConf conf = new SparkConf().setAppName("WordCount");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> input = context.textFile(outputFile);
        JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairRDD<String,Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s,1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer x, Integer y) throws Exception {
                return x + y;
            }
        });
        counts.saveAsTextFile(outputFile);
    }
}
