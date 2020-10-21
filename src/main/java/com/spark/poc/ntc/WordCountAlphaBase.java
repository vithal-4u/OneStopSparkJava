package com.spark.poc.ntc;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.spark.poc.utils.SparkUtils;

import scala.Tuple2;


public class WordCountAlphaBase {

	public static void main(String[] args) {
		String inputFile = args[0];
	    String outputFile = args[1];
	    final String toFind = "framework";
	    // Create a Java Spark Context.
	    System.setProperty("hadoop.home.dir", "D:\\Softwares\\winutils");
	    //SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local").set("spark.cores.max", "10");
	    SparkConf conf = SparkUtils.createSparkConfig("WordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// Load our input data.
	    JavaRDD<String> input = sc.textFile(inputFile);
	    // Split up into words.
	    input.repartition(4);
	    JavaRDD<String> words = input.flatMap(
	      new FlatMapFunction<String, String>() {
			public Iterator<String> call(String x) throws Exception {
				return Arrays.asList(x.split(" ")).iterator();
			}
	        
	     });

	    // Transform into word and count.
	    JavaPairRDD<String, String> dataList = words.mapToPair(
	  	      new PairFunction<String, String, String>(){
	  	        public Tuple2<String, String> call(String x){
	  	        	char alpha = x.charAt(0);
				return new Tuple2(Character.toString(alpha), x);
	  	        }
	  	      });
	    
	    JavaPairRDD<String, String> listAlpaData = dataList.reduceByKey(new Function2<String, String, String>() {
			@Override
			public String call(String v1, String v2) throws Exception {
				
				return v1+","+v2;
			}
		});
	    listAlpaData.partitionBy(new HashPartitioner(27));
	}

}
