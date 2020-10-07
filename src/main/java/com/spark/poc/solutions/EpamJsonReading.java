package com.spark.poc.solutions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.codehaus.janino.Java;

import com.spark.poc.pojo.EpamEmployeePOJO;
import com.spark.poc.utils.SparkUtils;

public class EpamJsonReading {

	public static void main(String[] args) {
		SparkSession spark = SparkUtils.createSparkSession("EpamJsonReading");
		Dataset<Row> employee = spark.read().json("D:\\Study_Document\\GIT\\OneStopPySpark\\resources\\Epam.json");
		employee.printSchema();
		
		employee.createOrReplaceTempView("Employee");

		// SQL statements can be run by using the sql methods provided by spark
		Dataset<Row> empDF = spark.sql("SELECT * FROM Employee");
		empDF.show();
		
		Dataset<EpamEmployeePOJO> EpamEmpDataset = 
				empDF.as(Encoders.bean(EpamEmployeePOJO.class));
		EpamEmpDataset.show();
		/*
		 * JavaRDD<EpamEmployeePOJO> rddEpamEmp = EpamEmpDataset.toJavaRDD();
		 * rddEpamEmp.map(new Function<EpamEmployeePOJO, R>() {
		 * 
		 * @Override public R call(EpamEmployeePOJO obj) throws Exception { // TODO
		 * Auto-generated method stub return null; } });
		 */
		
	}

}
