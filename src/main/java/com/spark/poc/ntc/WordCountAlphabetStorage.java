package com.spark.poc.ntc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.vector.util.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.spark.poc.utils.SparkUtils;

import scala.Tuple2;


public class WordCountAlphabetStorage {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String inputFile = args[0];
		String outputFile = args[1];
	    System.setProperty("hadoop.home.dir", "D:\\Softwares\\winutils");
	    SparkConf conf = SparkUtils.createSparkConfig("WordCountAlphabetStorage");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		// Load our input data.
	    JavaRDD<String> input = sc.textFile(inputFile);
	    JavaRDD<String> words = input.flatMap(
  	      new FlatMapFunction<String, String>() {
  			public Iterator<String> call(String x) throws Exception {
  				return Arrays.asList(x.split(" ")).iterator();
  			}
  	    });
	    JavaPairRDD<String, String> dataKeyVal = words.mapToPair(
  	      new PairFunction<String, String, String>(){
  	        public Tuple2<String, String> call(String x){
  	        	String firstChar = Character.toString(x.charAt(0));
  	          return new Tuple2(firstChar, x);
  	        }
  	    });
	   
	    JavaPairRDD<String, String> keyListVal = dataKeyVal.reduceByKey(
    		new Function2<String, String, String>(){
    			public String call(String x, String y){
    				String str = x +"," + y;
    				System.out.println("---------Data File---------"+ str);
    				return str;
    		}
	    });
	    StructType structType = DataTypes
	            .createStructType(
	                    new StructField[]{
	                            DataTypes.createStructField("AlphaChar", DataTypes.StringType, true)
	                            , DataTypes.createStructField("WordsVal", DataTypes.StringType, true)});
	    //create an RDD<Row> from pairRDD
	    JavaRDD<Row> rowJavaRDD = keyListVal.values().map(new Function<String, Row>() {
	        public Row call(String s) throws Exception {
                String key = Character.toString(s.charAt(0));
                Row row = RowFactory.create(key, s);
	            return row;
	        }
	    });
	    Dataset<Row> data =sqlContext.createDataFrame(rowJavaRDD, structType);
	    data.write().partitionBy("AlphaChar").parquet(outputFile);
	}

}
