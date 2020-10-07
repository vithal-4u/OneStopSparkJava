package com.spark.poc.solutions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.spark.poc.utils.CommonUtils;
import com.spark.poc.utils.SparkUtils;

/**
 * Created on 07-Oct-2020

    "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
    "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
    Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
    Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

    Example output:
    vagrant.vf.mmc.com
    www-a1.proxy.aol.com
    .....
    Keep in mind, that the original log files contains the following header lines.
    host    logname    time    method    url    response    bytes
    Make sure the head lines are removed in the resulting RDD.
    
 * @author Ashok Kumar
 *
 */
public class SameHostIntersectionSolution {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\Unzip_Softwares\\winutils\\bin");
		JavaSparkContext sparkCxt = SparkUtils.createJavaSparkContext("AirportsInUSSolutions");
		JavaRDD<String> julyFirstLogs = sparkCxt.textFile("D:\\Study_Document\\GIT\\OneStopSparkJava\\resources\\nasa_19950701.tsv");
		JavaRDD<String> augustFirstLogs = sparkCxt.textFile("D:\\Study_Document\\GIT\\OneStopSparkJava\\resources\\nasa_19950801.tsv");
		
		JavaRDD<String> julyFirstHost = julyFirstLogs.map(new Function<String, String>() {

			@Override
			public String call(String line) throws Exception {
				return CommonUtils.splitByTab(line)[0];
			}
		});
		
		JavaRDD<String> augustFirstHost = augustFirstLogs.map(new Function<String, String>() {

			@Override
			public String call(String line) throws Exception {
				return CommonUtils.splitByTab(line)[0];
			}
		});
		JavaRDD<String> hostIntersection = julyFirstHost.intersection(augustFirstHost);
		JavaRDD<String> filterHost = hostIntersection.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String line) throws Exception {
				return !line.equals("host");
			}
		});
		
		filterHost.saveAsTextFile("D:\\Study_Document\\GIT\\OneStopSparkJava\\output\\");

	}

}
