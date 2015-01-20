package com.embracesource.edh.hbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongStrColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;

public class AggregateTest {

  public static void main(String[] args) {
    Configuration conf = HBaseConfiguration.create();
    conf.setInt("hbase.client.retries.number", 1);
    conf.setInt("ipc.client.connect.max.retries", 1);
    
    byte[] table = Bytes.toBytes("t");
    Scan scan = new Scan();
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("id"));
    final ColumnInterpreter<Long, Long> columnInterpreter = new LongStrColumnInterpreter();

    try {
      AggregationClient aClient = new AggregationClient(conf);
      Long rowCount = aClient.min(table, columnInterpreter, scan);
      System.out.println("The result is " + rowCount);
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }
}
