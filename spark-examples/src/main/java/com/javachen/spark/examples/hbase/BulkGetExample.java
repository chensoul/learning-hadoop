package com.javachen.spark.examples.hbase;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.serde2.objectinspector.FullMapEqualComparer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.hbase.HBaseContext;
import org.apache.spark.hbase.JavaHBaseContext;

public class BulkGetExample {
	public static void main(String args[]) {

		String master = "local[2]";
		String tableName = "test";

		JavaSparkContext jsc = new JavaSparkContext(master,
				"JavaHBaseBulkGetExample");
		List<byte[]> list = new ArrayList<byte[]>();
		list.add(Bytes.toBytes("1"));
		list.add(Bytes.toBytes("2"));
		list.add(Bytes.toBytes("3"));
		list.add(Bytes.toBytes("4"));
		list.add(Bytes.toBytes("5"));

		JavaRDD<byte[]> rdd = jsc.parallelize(list);

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "localhost:2181");
		conf.set("zookeeper.znode.parent", "/hbase");

		JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

		JavaRDD<String> jrdd =  hbaseContext.bulkGet(tableName, 2, rdd, new GetFunction(),
				new ResultFunction());
		jrdd.foreach(new VoidFunction<String>() {
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
	}

	public static class GetFunction implements Function<byte[], Get> {

		private static final long serialVersionUID = 1L;

		public Get call(byte[] v) throws Exception {
			return new Get(v);
		}
	}

	public static class ResultFunction implements Function<Result, String> {

		private static final long serialVersionUID = 1L;

		public String call(Result result) throws Exception {
			List<Cell> it = result.listCells();
			StringBuilder b = new StringBuilder();

			b.append(Bytes.toString(result.getRow()) + ":");
			for (Cell cell : it) {
				String q = Bytes.toString(CellUtil.cloneQualifier(cell));
					b.append("(" + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
							+ Bytes.toString(CellUtil.cloneValue(cell)) + ")");
			}
			return b.toString();
		}
	}
}
