package com.wxy.bigdata.spark.examples;

import static org.apache.spark.sql.functions.col;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.examples.sql.JavaSparkSQLExample.Person;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSql {
	public static void main(String[] args) throws AnalysisException {
		SparkSession spark = SparkSession.builder().appName("Java Spark SQL Example")
				.config("spark.some.config.option", "some-value").master("local[2]").getOrCreate();
		runBasicDataFrameExample(spark);
		runDatasetCreationExample(spark);
		spark.stop();
	}

	private static void runBasicDataFrameExample(SparkSession spark) throws AnalysisException {
		Dataset<Row> df = spark.read().json("src/main/resources/people.json");
		df.show();

		df.printSchema();

		df.select("name").show();

		df.select(col("name"), col("age")).show();

		df.filter(col("age").gt(21)).show();

		df.groupBy("age").count();
		df.createOrReplaceTempView("people");

		Dataset<Row> sqlDf = spark.sql("select * from people");
		sqlDf.show();

		df.createGlobalTempView("people");

		spark.sql("SELECT * FROM global_temp.people").show();
		spark.newSession().sql("SELECT * FROM global_temp.people").show();
	}

	private static void runDatasetCreationExample(SparkSession spark) {
		Person person = new Person();
		person.setName("Andy");
		person.setAge(32);
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> javaBeanDS = spark.createDataset(Collections.singletonList(person), personEncoder);
		javaBeanDS.show();

		Encoder<Integer> integerEncoder = Encoders.INT();
		Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
		Dataset<Integer> transformedDS = primitiveDS.map((MapFunction<Integer, Integer>) value -> value + 1,
				integerEncoder);
		transformedDS.collect();

		String path = "src/main/resources/people.json";
		Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
		peopleDS.show();
	}

	public static class Person implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private String name;
		private int age;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}
	}

}
