package com.spark.poc.ntc;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.spark.poc.utils.SparkUtils;


public class SparkStreaming {
	

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\Softwares\\winutils");
		SparkConf sparkConfig = SparkUtils.createSparkConfig("Spark Streaming");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConfig, Durations.seconds(1));
	}
	
}
