package com.spark.poc.ntc;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import com.spark.poc.pojo.UserTable;

public class StreamingKafkaDummy {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\Softwares\\winutils");
		SparkConf conf = new SparkConf().setAppName("StreamingKafkaDummy").setMaster("local[2]").set("spark.sql.warehouse.dir", "D:\\Work_Bench\\spark-warehouse");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// Load our input data.
		JavaRDD<String> input = sc.textFile("D:\\MyPersonal\\Data\\PlayersStreaming.txt");
		System.out.println("Will Start executing FlatMap");
		SQLContext sqlContext = new SQLContext(sc);
		SparkSession spark = SparkSession.builder().appName("Spark SQL Test").master("local[2]").getOrCreate(); 
		Dataset<Row> df = sqlContext.read().json("examples/src/main/resources/people.json");
		
		Properties connectionProperties = new Properties();
		connectionProperties.put("driver", "com.mysql.jdbc.Driver");
		connectionProperties.put("url", "jdbc:mysql://localhost:3306/spark_db");
		connectionProperties.put("user", "root");
		connectionProperties.put("password", "mysql");
		Dataset<Row> jdbcDF = spark.read().jdbc(connectionProperties.getProperty("url"), "usertable", connectionProperties);
		Broadcast<Dataset<Row>> broadUser = sc.broadcast(jdbcDF);
		JavaRDD<UserTable> userData = input.flatMap(new FlatMapFunction<String, UserTable>() {
			public Iterator<UserTable> call(String x) throws Exception {
				String arrayData[] = x.split(",");
				UserTable user = new UserTable(arrayData[0], arrayData[1], arrayData[2], arrayData[3]);
				System.out.println("----Inside one Dataset----"+arrayData.toString());
				return Arrays.asList(user).iterator();
			}

		});
		
		Dataset<Row> userDataSet = sqlContext.createDataFrame(userData, UserTable.class);
		
		//userDataSet.write().mode(SaveMode.Append).jdbc(connectionProperties.getProperty("url"), "usertable", connectionProperties);
		userData.collect();
		
		jdbcDF.createOrReplaceTempView("usertable");
		jdbcDF.select("userId","votedTime").show();
		
		//userDf.write().mode(SaveMode.Append).jdbc(connectionProperties.getProperty("url"), "players", connectionProperties);
		
		
		/*spark.table("players").withColumn("userId", new Column("102"))
			.withColumn("voting_PlayerId", new Column("P11002"))
			.withColumn("voting_playerName", new Column("Suresh"))
			.withColumn("votedTime", new Column("2008-01-01 11:30:00"))
		.write().jdbc(connectionProperties.getProperty("url"), "players", connectionProperties);*/
		
		
		jdbcDF.show();
		jdbcDF.printSchema();
	}

}
