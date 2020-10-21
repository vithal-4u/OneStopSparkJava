package com.spark.poc.ntc;


import java.util.Iterator;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.spark.poc.utils.SparkUtils;

import scala.Tuple2;


public class RDDTransformationActions {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\Softwares\\winutils");
		
		SparkSession spark = SparkUtils.createSparkSession("Spark SQL Program");
		SparkContext sc = spark.sparkContext();
		JavaSparkContext javaSparkContext = SparkUtils.createJavaSparkContext("RDD Transformation Action");
		javaSparkContext.accumulator(0, "Count");
		//sc.broadcast(value, evidence$11)
		
		JavaRDD<Row> javaRDD = spark.read().json("D:\\Work_Bench\\data\\UserProfile.json").javaRDD();
		
		JavaRDD<Row> filterRDD = javaRDD.filter(new Function<Row, Boolean>() {
			
			public Boolean call(Row v1) throws Exception {
				return true;
			}
		});
		
		JavaRDD<String> javaPar = javaRDD.mapPartitions(new FlatMapFunction<Iterator<Row>, String>() {

			public Iterator<String> call(Iterator<Row> t) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
		});
		
		JavaPairRDD<String, String> javaPairRDD = javaRDD.mapToPair(new PairFunction<Row, String, String>() {

			@Override
			public Tuple2<String, String> call(Row row) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
		});
		
		JavaRDD<String> javaMap = javaRDD.map(new Function<Row, String>() {

			@Override
			public String call(Row v1) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
		});
		
		JavaRDD<String> javaFlatMap =javaRDD.flatMap(new FlatMapFunction<Row, String>() {

			@Override
			public Iterator<String> call(Row t) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
		});
		
		JavaDoubleRDD javaFlatMapDouble= javaRDD.flatMapToDouble(new DoubleFlatMapFunction<Row>() {
			
			@Override
			public Iterator<Double> call(Row t) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
		});
		
	}

}
