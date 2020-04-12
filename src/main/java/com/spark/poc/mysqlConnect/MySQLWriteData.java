package com.spark.poc.mysqlConnect;

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.spark.poc.utils.SparkUtils;

public class MySQLWriteData {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\Unzip_Softwares\\winutils");
		
		//Details related to MySQL connectivity
		Properties prop = new Properties();
		prop.setProperty("driver", "com.mysql.jdbc.Driver");
		prop.setProperty("dbtable", "Student_Marks");
		prop.setProperty("user", "root");
		prop.setProperty("password", "ayyappasai");
		
		SparkSession spark = SparkUtils.createSparkSession("MySQLWriteData");
		//Dataset<Row> csvDS = spark.read().format("csv").option("header","true").load("D:\\Study_Document\\GIT\\OneStopSparkJava\\resources\\student-dataset.csv");
		
		//# Creating DataType for each column  or schema for MySQL database
		StructField[] structFields = new StructField[]{
	            new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
	            new StructField("name", DataTypes.StringType, true, Metadata.empty()),
	            new StructField("nationality", DataTypes.StringType, true, Metadata.empty()),
	            new StructField("city", DataTypes.StringType, true, Metadata.empty()),
	            new StructField("gender", DataTypes.StringType, true, Metadata.empty()),
	            new StructField("age", DataTypes.IntegerType, true, Metadata.empty()),
	            new StructField("english_grade", DataTypes.FloatType, true, Metadata.empty()),
	            new StructField("math_grade", DataTypes.FloatType, true, Metadata.empty()),
	            new StructField("sciences_grade", DataTypes.FloatType, true, Metadata.empty()),
	            new StructField("portfolio_rating", DataTypes.IntegerType, true, Metadata.empty()),
	            new StructField("coverletter_rating", DataTypes.IntegerType, true, Metadata.empty()),
	            new StructField("refletter_rating", DataTypes.IntegerType, true, Metadata.empty())
	    };
		StructType structType = new StructType(structFields);
		
		JavaSparkContext javaContext = new JavaSparkContext(spark.sparkContext());
		JavaRDD<String> dataRDD= javaContext.textFile("D:\\Study_Document\\GIT\\OneStopSparkJava\\resources\\student-dataset.csv");
		String header = dataRDD.first();
		System.out.println(header);
		JavaRDD<String> withOutHeader = dataRDD.filter(row -> !row.equals(header));
		
		//# Reading Required element from each row for the CSV dataset
		JavaRDD<Row> rowRDD=withOutHeader.map(new Function<String, Row>() {
			@Override
			public Row call(String record) throws Exception {
				System.out.println(record);
				String[] arrayRecords = record.split(",");
				return RowFactory.create(Integer.parseInt(arrayRecords[0]),arrayRecords[1],arrayRecords[2],arrayRecords[3],arrayRecords[6],
						Integer.parseInt(arrayRecords[8]),Float.parseFloat(arrayRecords[9]),Float.parseFloat(arrayRecords[10]),
						Float.parseFloat(arrayRecords[11]),Integer.parseInt(arrayRecords[13]),Integer.parseInt(arrayRecords[14]),
						Integer.parseInt(arrayRecords[15]));
			}
		});
		
		//# Creating Dataset using filter RDD and Schema
		Dataset<Row> df = spark.createDataFrame(rowRDD, structType);
		df.show();
		
		//# Insert data into MySQL DB
		df.write().mode(SaveMode.Append)
			.jdbc("jdbc:mysql://localhost:3306/ONESTOP_SPARK_DB", "Student_Marks", prop);
		
		System.out.println("Success Fully Stored Data");
			
		
	}

	
}
