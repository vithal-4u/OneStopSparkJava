package com.spark.poc.ntc;

import java.io.DataInputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.input.StreamInputFormat;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class SparkSimpleStreaming {

	public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("Streaming HDFS path").setMaster("local[2]");

        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    
        JavaStreamingContext jstreamCtxt = new JavaStreamingContext(sparkConf, Seconds.apply(5)); //every 5 seconds
        
        JavaDStream<String> fileStream = jstreamCtxt.textFileStream("D:\\MyPersonal\\Data\\SparkStreaming");
        
        JavaDStream<String> words = fileStream.flatMap(
                new FlatMapFunction<String, String>(){
                    private static final long serialVersionUID = 1L;
                    public Iterator<String> call(String s){
                        return Arrays.asList(s.split(",")).iterator();
                        
                    }
                }
        );
        words.foreachRDD(new Function<JavaRDD<String>, Void>(){
            /**
             * 
             */
            private static final long serialVersionUID = 1L;
            
            public Void call(JavaRDD<String> rdd) throws Exception{
                if(rdd != null){
                    final List<String> result = rdd.collect();
                    System.out.println("Element:" + result.get(0));
                    System.out.println("Element:" + result.get(1));
                    System.out.println("Element:" + result.get(2));
                    
                }
                return null;
            }
        });

        /*fileStream.foreachRDD(x -> {
            List<Tuple2<String, PortableDataStream>> dataWithFileNames = x.collect();

            for (Tuple2<String, PortableDataStream> dataAndFileNm : dataWithFileNames) {

                String filePath = dataAndFileNm._1.split(":")[1];
                PortableDataStream data = dataAndFileNm._2;

                DataInputStream dis = data.open();
                byte[] bs = new byte[dis.available()];

                dis.readFully(bs);

                System.out.println("###### file " + filePath);

                System.out.println("###### data " + new String(bs));

                Files.deleteIfExists(Paths.get(filePath));
            }

        });*/

        jstreamCtxt.start();
        jstreamCtxt.awaitTermination();

        jstreamCtxt.close();

    }

}
