package com.spark.poc.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkUtils {
	public static SparkConf sparkConf;
	public static SparkSession sparkSession;
	public static SparkConf createSparkConfig(String appName) {
		if(sparkConf == null) {
			sparkConf = new SparkConf()
					.setAppName(appName)
					.setMaster("local[4]")
					.set("spark.sql.warehouse.dir",
							"D:\\Study_Document\\MyPersonal\\spark-warehouse");
		}
		return sparkConf;
	}

	public static SparkSession createSparkSession(String appName) {
		/**
		SparkSession sparkSession = SparkSession
				.builder()
				.appName(appName)
				.master("local[2]")
				.config("spark.driver.allowMultipleContexts", "true")
				.config("spark.sql.warehouse.dir",
						"D:\\Study_Document\\MyPersonal\\spark-warehouse").getOrCreate();
						
		*/
		if(sparkSession == null) {
			sparkSession = SparkSession
					.builder()
					.appName(appName)
					.master("local[2]")
					.config(SparkUtils.createSparkConfig(appName)).getOrCreate();
		}
		return sparkSession;
	}
	
	public static JavaSparkContext createJavaSparkContext(String appName) {
		return new JavaSparkContext(createSparkConfig(appName));
	}
}
