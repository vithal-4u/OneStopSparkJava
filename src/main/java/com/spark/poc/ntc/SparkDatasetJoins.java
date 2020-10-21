package com.spark.poc.ntc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.spark.poc.utils.SparkUtils;

import static org.apache.spark.sql.functions.col;

public class SparkDatasetJoins {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\Softwares\\winutils");
		SparkSession session = SparkUtils.createSparkSession("SparkDatasetJoins");
		Dataset<Row> purchase = session.read().option("header", "true")
				.csv("D:\\Work_Bench\\data\\purchase.csv");
		Dataset<Row> customer = session.read().option("header", "true")
				.csv("D:\\Work_Bench\\data\\customer.csv");
		Dataset<Row> book = session.read().option("header", "true")
				.csv("D:\\Work_Bench\\data\\book.csv");
		
		Dataset<Row> purcBookData = purchase.join(book,purchase.col("isbn").equalTo(book.col("isbn")))
					.join(customer,customer.col("cid").equalTo(purchase.col("cid")))
					.where(purchase.col("shop").equalTo("Amazon"));
	        /*.join(customer, customer("cid")===purchase("cid"))
	        .where(customer("name") !== "Harry Smith")
	        .join(temp, purchase("isbn")===temp("purchase_isbn"))
	        .select(customer("name").as("NAME")).distinct();*/
		purcBookData.show();
	}

}
