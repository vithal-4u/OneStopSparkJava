package com.spark.poc.solutions;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;

import com.spark.poc.utils.CommonUtils;
import com.spark.poc.utils.SparkUtils;

/**
 * 
 * Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,     
 * 	print the sum of those numbers to console.    
 * 	Each row of the input file contains 10 prime numbers separated by spaces.
 * 
 * @author Ashok Kumar
 *
 */
public class SumOfNumbersSolution {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\Unzip_Softwares\\winutils\\bin");
		JavaSparkContext sparkCxt = SparkUtils.createJavaSparkContext("SumOfNumbersSolution");
		JavaRDD<String> numberLines = sparkCxt.textFile("D:\\Study_Document\\GIT\\OneStopSparkJava\\resources\\prime_nums.text");
		JavaRDD<Integer> numbersOnly = numberLines.flatMap(new FlatMapFunction<String, Integer>() {
			@Override
			public Iterator<Integer> call(String line) throws Exception {
				return Arrays.asList(CommonUtils.splitByTabInteger(line)).iterator();
			}

		});
		
		Integer sum = numbersOnly.reduce(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer num1, Integer num2) throws Exception {
				return num1+num2;
			}
		});
		
		System.out.println("Total Sum ==="+ sum);
	}

}
