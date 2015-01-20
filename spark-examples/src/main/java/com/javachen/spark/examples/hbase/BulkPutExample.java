package com.javachen.spark.examples.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.hbase.JavaHBaseContext;

public class BulkPutExample {
	 
  public static void main(String args[]) {

    String master = "local[2]";
    String tableName = "test";
    String columnFamily = "f";

    JavaSparkContext jsc = new JavaSparkContext(master,
        "JavaHBaseBulkPutExample1");

    List<String> list = new ArrayList<String>();
    list.add("1," + columnFamily + ",a,1");
    list.add("2," + columnFamily + ",a,2");
    list.add("3," + columnFamily + ",a,3");
    list.add("4," + columnFamily + ",a,4");
    list.add("5," + columnFamily + ",a,5");

    JavaRDD<String> rdd = jsc.parallelize(list);

    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "localhost:2181");
    conf.set("zookeeper.znode.parent", "/hbase");

    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

    hbaseContext.bulkPut(rdd, tableName, new PutFunction(), true);
  }

  public static class PutFunction implements Function<String, Put> {

    private static final long serialVersionUID = 1L;

    public Put call(String v) throws Exception {
      String[] cells = v.split(",");
      Put put = new Put(Bytes.toBytes(cells[0]));

      put.add(Bytes.toBytes(cells[1]), Bytes.toBytes(cells[2]),
          Bytes.toBytes(cells[3]));
      put.add(Bytes.toBytes(cells[1]), Bytes.toBytes("test1"),
              Bytes.toBytes("test"));
      return put;
    }
  }
}
