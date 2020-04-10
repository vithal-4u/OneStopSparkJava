package com.spark.java.poc.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkUtils {
	public static SparkConf createSparkConfig(String appName) {
		SparkConf sparkConf = new SparkConf()
				.setAppName(appName)
				.setMaster("local[4]")
				.set("spark.sql.warehouse.dir",
						"D:\\MyPersonal\\spark-warehouse");
		return sparkConf;
	}

	public static SparkSession createSparkSession(String appName) {
		SparkSession sparkSession = SparkSession
				.builder()
				.appName(appName)
				.master("local[2]")
				.config("spark.sql.warehouse.dir",
						"D:\\MyPersonal\\spark-warehouse").getOrCreate();
		return sparkSession;
	}
	
	public static JavaSparkContext createJavaSparkContext(String appName) {
		return new JavaSparkContext(createSparkConfig(appName));
	}
}
