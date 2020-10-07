package com.spark.poc.solutions;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import com.spark.poc.utils.CommonUtils;
import com.spark.poc.utils.SparkUtils;

/**
 * 
 *  Created on 06-Oct-2020
    Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
    Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

    Each row of the input file contains the following columns:
        Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code, ICAO Code,
        Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

    Sample output:
    "St Anthony", 51.391944
    "Tofino", 49.082222
   
 * @author kasho
 *
 */
public class AirportsByLatitudeSolutions {
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\Unzip_Softwares\\winutils\\bin");
		JavaSparkContext sparkCxt = SparkUtils.createJavaSparkContext("AirportsByLatitudeSolutions");
		JavaRDD<String> linesRDD = sparkCxt.textFile("D:\\Study_Document\\GIT\\OneStopSparkJava\\resources\\airports.txt");
		JavaRDD<String> filteredLinesRDD = linesRDD.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String line) throws Exception {
				String arryLine[] = CommonUtils.splitByComma(line);
				return new Float(arryLine[6]) > 40;
			}
		});
		
		JavaRDD<Row> filteredAirports = filteredLinesRDD.map(new Function<String, Row>() {

			@Override
			public Row call(String line) throws Exception {
				String arryLine[] = CommonUtils.splitByComma(line);
				return RowFactory.create(arryLine[1],Float.parseFloat(arryLine[6]));
			}
		});
		
		filteredAirports.saveAsTextFile("D:\\Study_Document\\GIT\\OneStopSparkJava\\output\\");
		
		
	}
}
