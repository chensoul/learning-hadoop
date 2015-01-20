package com.javachen.spark.examples.ftp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class FtpExample {
	
	public static void main(String args[]) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local[2]");
		conf.setAppName("ftp");
		JavaSparkContext jsc = new JavaSparkContext("local[1]", "FtpExample");
		JavaRDD rdd = jsc.textFile("ftp://anonymous:anonymous@localhost/pub/README.txt");
		System.out.println(rdd.count());
	}
}
