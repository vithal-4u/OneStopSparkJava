package com.spark.poc.ntc;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.spark_project.guava.collect.Ordering;

import com.spark.poc.utils.SparkUtils;

import scala.Tuple2;

public class SparkJSON {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\Softwares\\winutils");
		
		SparkSession spark = SparkUtils.createSparkSession("Spark SQL Program");
		//sc.broadcast(value, evidence$11)
		JavaRDD<Row> javaRDD = spark.read().json("D:\\Work_Bench\\data\\UserProfile.json").javaRDD();
		javaRDD.partitions();
		/*Dataset<Row> df = spark.read().json("D:\\Work_Bench\\data\\UserProfile.json");
		df.show();*/
		JavaPairRDD<Integer, String> pairKeyVal = javaRDD.mapToPair(new PairFunction<Row, Integer, String>() {

			public Tuple2<Integer, String> call(Row row) throws Exception {
				System.out.println("UserID--"+row.getAs("userId")+" --text-- "+row.getAs("text")+" --Stream-- "+ row.getAs("Stream"));
				String text[] = row.getAs("text").toString().split(" ");
				int count=0;
				for(String eachStr : text){
					if(eachStr.contains("#EPAM")){
						count++;
					}
				}
				return new Tuple2<Integer, String>(count, row.getAs("userId").toString());
			}
		});
		JavaPairRDD<Integer, String> answer = pairKeyVal.sortByKey();
		System.out.println(answer.countByValue());
		List<Tuple2<Integer, String>> t2 = answer.take(1);
		System.out.println(t2.get(0)._2());
		//List<Tuple2<Integer, String>> listTuple = pairKeyVal.collect();
		//System.out.println("List---- "+ listTuple);
		/*JavaRDD<String> strRows = javaRDD.map(new Function<Row, String>() {
			public String call(Row row) throws Exception {
				System.out.println(row.mkString());
				return row.mkString();
			}
		});*/
		/*JavaPairRDD<String, String> pairKeyVal = strRows.flatMapToPair(new PairFlatMapFunction<String, String, String>() {

			public Iterator<Tuple2<String, String>> call(String row)
					throws Exception {
				
				return null;
			}
			
		});*/
		/* List<String> listRows = strRows.collect();
		System.out.println("----List Of Rows---- "+listRows.size());
		JavaRDD<Row> items = spark.read().json("D:\\Work_Bench\\data\\UserProfile.json").toJavaRDD();
	        items.foreach(new VoidFunction<Row>() {
				
				public void call(Row row) throws Exception {
					System.out.println(row.mkString());
				}
			});*/
		
		/*df.flatMap(new FlatMapFunction<String, String>() {

			public Iterator<String> call(String row) throws Exception {
				System.out.println();
				return null;
			}
		});*/
		
		
		// read list to RDD
       /* String jsonPath = "D:\\Work_Bench\\data\\Sample.json";
        JavaRDD<Row> items = spark.read().json(jsonPath).toJavaRDD();
        items.foreach(new VoidFunction<Row>() {
			
			public void call(Row row) throws Exception {
				System.out.println(row.mkString());
			}
		});*/
	}
	
}
