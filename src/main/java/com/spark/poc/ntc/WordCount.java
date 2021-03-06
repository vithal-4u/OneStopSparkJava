package com.spark.poc.ntc;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.lang.Iterable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.spark.poc.utils.SparkUtils;

import org.apache.spark.api.java.function.Function;


import scala.Tuple2;

public class WordCount {

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
	   /* words.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {

			public Iterator<String> call(Iterator<String> t) throws Exception {
				return null;
			}
		});
	    
	    JavaRDD<String> inputFiltered = words.filter(
			new Function<String, Boolean>() {
				public Boolean call(String v1) throws Exception {
					return toFind.equals(v1);
				}
			});
	    			*/

	    // Transform into word and count.
	    JavaPairRDD<String, Integer> dataList = words.mapToPair(
	  	      new PairFunction<String, String, Integer>(){
	  	        public Tuple2<String, Integer> call(String x){
	  	          return new Tuple2(x, 1);
	  	        }
	  	      });
	    
	    JavaPairRDD<String, Integer> counts = dataList.reduceByKey(
	    		new Function2<Integer, Integer, Integer>(){
	    			public Integer call(Integer x, Integer y){ 
	    				System.out.println("Inside--X"+ x + "-- Y ---"+y );
	    				return x + y;
	    		}
	    });
	    
	    // Save the word count back out to a text file, causing evaluation.
	    counts.saveAsTextFile(outputFile);
	}

}
