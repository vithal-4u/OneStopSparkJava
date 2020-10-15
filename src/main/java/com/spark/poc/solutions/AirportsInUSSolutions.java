package com.spark.poc.solutions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import com.spark.poc.utils.CommonUtils;
import com.spark.poc.utils.SparkUtils;

/**
 * 
 *Created on 07-Oct-2020
	Problem Requirment:
	    Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
	    and output the airport's name and the city's name to out/airports_in_usa.text. Each row of the input file contains the following columns:
	    Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code, ICAO Code, Latitude,
	    Longitude, Altitude, Timezone, DST, Timezone in Olson format
	
	    Sample output:
	    "Putnam County Airport", "Greencastle"
	    "Dowagiac Municipal Airport", "Dowagiac"
	    
 * @author: Ashok Kumar
 *
 */
public class AirportsInUSSolutions {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\Unzip_Softwares\\winutils\\bin");
		JavaSparkContext sparkCxt = SparkUtils.createJavaSparkContext("AirportsInUSSolutions");
		JavaRDD<String> linesRDD = sparkCxt.textFile("D:\\Study_Document\\GIT\\OneStopSparkJava\\resources\\airports.txt");
		JavaRDD<String> filteredLinesRDD = linesRDD.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String line) throws Exception {
				String arryLine[] = CommonUtils.splitByComma(line);
				return arryLine[3].equals("\"United States\"");
			}
		});
		
		JavaRDD<Row> filteredAirports = filteredLinesRDD.map(new Function<String, Row>() {

			@Override
			public Row call(String line) throws Exception {
				String arryLine[] = CommonUtils.splitByComma(line);
				return RowFactory.create(arryLine[1],arryLine[2]);
			}
		});
		
		filteredAirports.coalesce(1).saveAsTextFile("D:\\Study_Document\\GIT\\OneStopSparkJava\\output\\");
		
		
	}

}
