package com.wxy.bigdata.spark.ecommerce;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import com.wxy.bigdata.spark.ecommerce.util.ConfigurationManager;
import com.wxy.bigdata.spark.ecommerce.util.Constants;


/**
 * 用户访问session分析Spark作业
 *
 * */
public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {
        
        SparkSession spark = SparkSession.builder().appName(Constants.SPARK_APP_NAME_SESSION)
				.config("spark.some.config.option", "some-value").master("local[2]").getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        mockData(jsc,spark.sqlContext());
        spark.stop();
    }


    /**
     * 生成模拟数据（只有本地模式，才会去生成模拟数据）
     * @param sc
     * @param sqlContext
     */
    private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local) {
            MockData.mock(sc, sqlContext);
        }
    }
}