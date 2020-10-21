package com.spark.poc.ntc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.spark.poc.utils.SparkUtils;

import scala.Function1;


public class SparkSQLEmployeeCompn {

	public static <U> void main(String[] args) throws AnalysisException {
		System.setProperty("hadoop.home.dir", "D:\\Softwares\\winutils");

		SparkSession session = SparkUtils.createSparkSession("SparkSQLEmployeeCompn");
		Dataset<Row> df = session.read().option("header", "true")
				.csv("D:\\MyPersonal\\Data\\Employee_Compensation.csv");
		
		JavaRDD<String> rdd = session.read().option("header", "true")
			.textFile("D:\\MyPersonal\\Data\\Employee_Compensation.csv").javaRDD();
		
		//rdd.saveAsTextFile("D:\\MyPersonal\\output\\1");
		 List<String> headers = Arrays.asList(rdd.take(1).get(0).split(","));
		// headers.forEach(System.out :: println);
		 JavaRDD<String> headerModRows=rdd.map(new Function<String, String>() {
				@Override
				public String call(String row) throws Exception {
					System.out.println("Inside Map ---" + row);
					return row;
				}
			});
		 headerModRows.count();
		/* JavaRDD<String> rddStr = rdd.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String t) throws Exception {
				System.out.println("Each Row --- "+t);
				return Arrays.asList(t).iterator();
			}
		});*/
		 //rddStr.count();
		 /*
		JavaRDD<Row> headerModRows=rdd.map(new Function<Row, Row>() {
			@Override
			public Row call(Row row) throws Exception {
				row.
				return null;
			}
		});
		
		Dataset<Row> newDF = df;
		String colArry[] =df.columns();
		for(String col:colArry) {
			newDF = newDF.withColumnRenamed(col, col.replaceAll(" ", "_"));
		}
		newDF.show(100);
		newDF.createTempView("Employee_Compensation");
		Dataset<Row> dfYears = session.sql("select * from Employee_Compensation where Year='2013'");
		System.out.println("---------- Dataset With Years 2013 ------------");
		dfYears.show(50);
		Dataset<Row> dfYearsOrderByDesc = session.sql("select * from Employee_Compensation where Year='2013' order by Department_Code asc");
		//dfYears.sort(asc(""));
		System.out.println("---------- Dataset With Years 2013 with order by Department_Code asc ------------");
		dfYearsOrderByDesc.show(50);
		*/
	}

}
