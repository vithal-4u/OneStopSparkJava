package com.spark.poc.ntc;

import org.apache.spark.sql.SparkSession;

import com.spark.poc.utils.SparkUtils;


public class SparkJoinOperations {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\Softwares\\winutils");
		
		SparkSession spark = SparkUtils.createSparkSession("Spark Join Program");
		
	}

}
