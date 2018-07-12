package com.wxy.bigdata.hadoop.wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class WordCount {
    public static class Map extends MapReduceBase implements Mapper<LongWritable,Text, Text,IntWritable> {
        private Text word = new Text();
        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            String line = text.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while(tokenizer.hasMoreElements()){
                word.set(tokenizer.nextToken());
                outputCollector.collect(word,new IntWritable(1));
            }
        }
    }
    public static class Reduce extends MapReduceBase implements Reducer<Text,IntWritable, Text,IntWritable> {


        @Override
        public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            int sum = 0;
            while(iterator.hasNext()){
                sum += iterator.next().get();
            }
            outputCollector.collect(text,new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("job-WordCount");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf,new Path(args[0]));
        FileOutputFormat.setOutputPath(conf,new Path(args[1]));

        JobClient.runJob(conf);
    }
}
