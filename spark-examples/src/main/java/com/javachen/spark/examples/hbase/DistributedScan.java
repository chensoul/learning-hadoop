package com.javachen.spark.examples.hbase;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.hbase.JavaHBaseContext;

import scala.Tuple2;
import scala.Tuple3;

public class DistributedScan {
	public static void main(String args[]) {

		String master = "local[2]";
		String tableName = "test";

		JavaSparkContext jsc = new JavaSparkContext(master,
				"JavaHBaseDistributedScan");

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "localhost:2181");
		conf.set("zookeeper.znode.parent", "/hbase");
		

		JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

		Scan scan = new Scan();
		scan.setCaching(100);

		JavaRDD<Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>>> javaRdd = hbaseContext
				.hbaseRDD(tableName, scan);
		
		javaRdd.foreach(new VoidFunction<Tuple2<byte[],List<Tuple3<byte[],byte[],byte[]>>>>(){

			@Override
			public void call(
					Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>> t)
					throws Exception {
				System.out.println("Row:"+Bytes.toString(t._1));
				for (Tuple3<byte[], byte[], byte[]> r : t._2) {
					System.out.println("value:"+Bytes.toString(r._1())+":"+Bytes.toString(r._2())+"=>"+Bytes.toString(r._3()));
				}
			}});
		
	}
}
