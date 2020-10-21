package com.spark.poc.ntc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.json.JSONArray;
import org.json.JSONObject;

import scala.Tuple2;

import com.spark.poc.utils.SparkUtils;
import com.spark.pojo.RelativeTuple;

public class RelativeStrengthIndex {

	public static void main(String[] args) throws InterruptedException {
		System.setProperty("hadoop.home.dir", "D:\\Softwares\\winutils");
		final String[] path = new String[] { "D:\\MyPersonal\\Data\\input1",
				"D:\\MyPersonal\\Data\\out" };
		SparkConf sparkConf = SparkUtils
				.createSparkConfig("RelativeStrengthIndex");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		JavaStreamingContext jstream = new JavaStreamingContext(jsc,
				new Duration(10000));
		JavaDStream<String> dstream = jstream.textFileStream(path[0]);
		
		JavaDStream<String> wordDStreamRDD = dstream.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String x) throws Exception {
				return Arrays.asList(x.split(" ")).iterator();
			}
		});
		
		JavaPairDStream<String, Integer> keyValPairRDD = wordDStreamRDD.mapToPair(
  	      new PairFunction<String, String, Integer>(){
  	        public Tuple2<String, Integer> call(String x){
  	        	System.out.println("Word --- "+ x);
  	          return new Tuple2(x, 1);
  	        }
  	      });
		
		JavaPairDStream<String, Integer> counts = keyValPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>(){
	    			public Integer call(Integer x, Integer y){ 
	    				System.out.println("Inside--X"+ x + "-- Y ---"+y );
	    				return x + y;
	    		}
		});
		counts.print();
		/*JavaPairDStream<String, RelativeTuple> pair = dstream
				.flatMapToPair(new PairFlatMapFunction<String, String, RelativeTuple>() {
					*//**
				*
				*//*
					private static final long serialVersionUID = 67676744;

					public Iterator<Tuple2<String, RelativeTuple>> call(String t)
							throws Exception {
						List<Tuple2<String, RelativeTuple>> list = new ArrayList<Tuple2<String, RelativeTuple>>();
						JSONArray js1 = new JSONArray(t);
						for (int i = 0; i < js1.length(); i++) {
							String symbol = js1.getJSONObject(i).get("symbol")
									.toString();
							System.out.println("Symbol----"+ symbol);
							JSONObject jo = new JSONObject(js1.getJSONObject(i)
									.get("priceData").toString());
							Double closingPrice = jo.getDouble("close");
							Double openingPrice = jo.getDouble("open");
							RelativeTuple relativeTuple = null;
							if (closingPrice > openingPrice) {
								relativeTuple = new RelativeTuple(1, 0,
										closingPrice, 0);
							} else {
								relativeTuple = new RelativeTuple(0, 1,
										openingPrice, 0);
							}
							list.add(new Tuple2<String, RelativeTuple>(symbol,
									relativeTuple));
						}
						return list.iterator();
					}
				});
		JavaPairDStream<String, RelativeTuple> result = pair
				.reduceByKeyAndWindow(
						new Function2<RelativeTuple, RelativeTuple, RelativeTuple>() {
							private static final long serialVersionUID = 76761212;

							public RelativeTuple call(RelativeTuple result,
									RelativeTuple value) throws Exception {
								System.out.println("Window Operation------");
								result.setUpward(result.getUpward()
										+ value.getUpward());
								result.setUpwardCount(result.getUpwardCount()
										+ value.getUpwardCount());
								result.setDownward(result.getDownward()
										+ value.getDownward());
								result.setDownwardCount(result
										.getDownwardCount()
										+ value.getDownwardCount());
								return result;
							}
						}, new Duration(600000), new Duration(300000));
		result.foreachRDD(new VoidFunction<JavaPairRDD<String, RelativeTuple>>() {
			*//**
				*
				*//*
			private static final long serialVersionUID = 6767679;

			public void call(JavaPairRDD<String, RelativeTuple> t)
					throws Exception {
				t.coalesce(1).saveAsTextFile(
						path[1] + java.io.File.separator
								+ System.currentTimeMillis());
			}
		});*/
		jstream.start();
		jstream.awaitTermination();
	}

}
