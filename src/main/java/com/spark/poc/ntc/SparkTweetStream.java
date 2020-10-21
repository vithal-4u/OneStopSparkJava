package com.spark.poc.ntc;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.*;

import com.spark.poc.utils.SparkUtils;

import twitter4j.GeoLocation;
import twitter4j.Status;


public class SparkTweetStream {

	public static void main(String[] args) throws InterruptedException {
		System.setProperty("hadoop.home.dir", "D:\\Softwares\\winutils");
		final String consumerKey = "ZKgUdO6OHHNSFYhabgiO7G4oX";
        final String consumerSecret = "t99i3R8PxaHNGmHJwpx0isghEKIGgq4pwC4DAQgSy6wJFPpDtQ";
        final String accessToken = "811420957-WKIiYwZH1fgcyYTq6rLz2lGOzopgAZgr5lq6lsbV";
        final String accessTokenSecret = "Y3RETpClojyGdvfBVW48wCreLMWjZuqtRjsmWVnpgmN4y";

        
        SparkConf conf = SparkUtils.createSparkConfig("SparkTwitterHelloWorldExample");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(30000));

        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc);

        // Without filter: Output text of all tweets
        JavaDStream<String> statuses = twitterStream.map(
                new Function<Status, String>() {
                    public String call(Status status) { return status.getText(); }
                }
        );

        // With filter: Only use tweets with geolocation and print location+text.
        /*JavaDStream<Status> tweetsWithLocation = twitterStream.filter(
                new Function<Status, Boolean>() {
                    public Boolean call(Status status){
                        if (status.getGeoLocation() != null) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                }
        );
        JavaDStream<String> statuses = tweetsWithLocation.map(
                new Function<Status, String>() {
                    public String call(Status status) {
                        return status.getGeoLocation().toString() + ": " + status.getText();
                    }
                }
        );*/

        statuses.print();
        jssc.start();
        jssc.awaitTermination();
    }

}
