package com.wxy.bigdata.spark.examples;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import java.util.Arrays;
import java.util.Iterator;
/**
 * Created by syorn on 2018/07/10.
 */
public class PeopleInfoCalculator {
    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf().setAppName("PeopleInfoCalculator").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> dataFile = sc.textFile("F:\\PeopleInfo.txt");

        JavaRDD<String> maleFilterData = dataFile.filter(new Function<String, Boolean>() {//过滤出性别为M的数据
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("M");
            }
        });
        JavaRDD<String> femaleFilterData = dataFile.filter(new Function<String, Boolean>() {//过滤出性别为F的数据
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("F");
            }
        });
        JavaRDD<String> maleHeightData = maleFilterData.flatMap(new FlatMapFunction<String, String>() {//得到性别为M的身高数据
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")[2]).iterator();
            }
        });
        JavaRDD<String> femaleHeightData = femaleFilterData.flatMap(new FlatMapFunction<String, String>() {//得到性别为F的身高数据
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")[2]).iterator();
            }
        });
        JavaRDD<Integer> maleHeightDataInt = maleHeightData.map(new Function<String, Integer>() {//将字符串格式转化为整型格式
            @Override
            public Integer call(String s) throws Exception {
                return Integer.parseInt(String.valueOf(s));
            }
        });
        JavaRDD<Integer> femaleHeightDataInt = femaleHeightData.map(new Function<String, Integer>() {//将字符串格式转化为整型格式
            @Override
            public Integer call(String s) throws Exception {
                return Integer.parseInt(String.valueOf(s));
            }
        });
        //sortBy(<T>,ascending,numPartitions) 解释:
        //第一个参数是一个函数，该函数的也有一个带T泛型的参数，返回类型和RDD中元素的类型是一致的；
        //第二个参数是ascending，这参数决定排序后RDD中的元素是升序还是降序，默认是true，也就是升序；
        //第三个参数是numPartitions，该参数决定排序后的RDD的分区个数，默认排序后的分区个数和排序之前的个数相等，即为this.partitions.size。
        JavaRDD<Integer> maleHeightLowSort = maleHeightDataInt.sortBy(new Function<Integer,Integer>(){// true表示默认排序，为升序排序，从低到高排
            public Integer call(Integer s) throws Exception {
                return s;
            }
        },true,3);
        JavaRDD<Integer> femaleHeightLowSort = femaleHeightDataInt.sortBy(new Function<Integer,Integer>(){// true表示默认排序，为升序排序，从低到高排
            public Integer call(Integer s) throws Exception {
                return s;
            }
        },true,3);
        JavaRDD<Integer> maleHeightHightSort = maleHeightDataInt.sortBy(new Function<Integer,Integer>(){// false表示为降序排序，从高到低
            public Integer call(Integer s) throws Exception {
                return s;
            }
        },false,3);
        JavaRDD<Integer> femaleHeightHightSort = femaleHeightDataInt.sortBy(new         Function<Integer,Integer>(){// true表示默认排序，为降序排序，从低到高排
            public Integer call(Integer s) throws Exception {
                return s;
            }
        },false,3);
        Integer lowestMale = maleHeightLowSort.first(); //求出升序的第一个数，即最小值
        Integer lowestFemale = femaleHeightLowSort.first();//求出升序的第一个数，即最小值
        Integer highestMale = maleHeightHightSort.first();//求出降序的第一个数，即最大值
        Integer highestFemale = femaleHeightHightSort.first();//求出降序的第一个数，即最大值

        System.out.println("Number of Female Peole:" + femaleHeightData.count());//求出女性的总个数
        System.out.println("Number of Male Peole:" + maleHeightData.count());//求出男性的总个数
        System.out.println("Lowest Male:" + lowestMale);//求出男性最矮身高
        System.out.println("Lowest Female:" + lowestFemale);//求出女性最矮身高
        System.out.println("Highest Male:" + highestMale);//求出男性最高身高
        System.out.println("Highest Female:" + highestFemale);//求出女性最高身高

    }
}