package com.spark.poc.ntc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLLib {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\Softwares\\winutils");
		SparkSession spark = SparkSession
				  .builder()
				  .appName("Java Spark SQL basic example")
				  .config("spark.some.config.option", "some-value")
				  .master("local[2]")
				  .getOrCreate();
		Dataset<Row> df = spark.read().option("header", "true").csv("D:\\MyPersonal\\Data\\SalesRecords\\Sales_Records_1k.csv");
		df.show(25);
		df.printSchema();
		df.select("Region","Order ID").show(10);
		df.createOrReplaceTempView("salesRecords");
		Dataset<Row> sqlDF = spark.sql("Select * from salesRecords");
		sqlDF.show(10);
	}

}
