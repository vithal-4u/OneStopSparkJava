package com.spark.poc.mysqlConnect;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.spark.poc.utils.SparkUtils;

public class MySQLReadData {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\Unzip_Softwares\\winutils");
		SparkSession spark = SparkUtils.createSparkSession("MySQLReadData");
		Dataset<Row> jdbcDF = spark.read().format("jdbc").option("url", "jdbc:mysql://localhost:3306/sakila")
			.option("driver", "com.mysql.jdbc.Driver")
			.option("dbtable", "actor")
			.option("user", "root")
			.option("password", "ayyappasai")
			.load();
		jdbcDF.show(100);
		
	}

}
