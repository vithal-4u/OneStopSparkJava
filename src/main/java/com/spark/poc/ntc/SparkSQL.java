package com.spark.poc.ntc;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import com.spark.poc.utils.SparkUtils;

import static org.apache.spark.sql.functions.col;

public class SparkSQL {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\Softwares\\winutils");
		
		SparkSession spark = SparkUtils.createSparkSession("Spark SQL Program");
		Dataset<Row> df = spark.read().csv("D:\\Work_Bench\\spark-master\\examples\\src\\main\\resources\\people.csv")
				.toDF("name","age","job");
		
		df.show(10);
		df.select("name","age","job").show();
		Dataset<Row> data = df.select("name","age","job");
		 data = data.withColumn("email", functions.lit("k.ashokachary@gmail.com"));
		 data.show();
		//df.select(df.col("*")).where(df.col("name").equalTo("Ashok")).show();
		
		df.select(col("name"), col("age").plus(1)).show();
		df.write().partitionBy("age").save("D:\\MyPersonal\\Data\\output4");
		//Dataset<Row> data = df.select("name");
		
		//df.filter(col("age").gt(21)).show();
		/*df.select(col("name"), col("age").plus(1));
		df.sort(col("age")).show();
		df.orderBy(col("name").asc()).sort(col("age")).show();*/
	}

}
