package com.embracesource.edh.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class ParallelScannerTest {

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    HTable table = new HTable(conf, "t");
    String startKey = "1";
    String stopKey = "3";
    boolean isParallel = true;
    String familyName = "f";
    String columnName = "id";
    String remainder = "2";

    Scan scan = new Scan(Bytes.toBytes(startKey), Bytes.toBytes(stopKey));
    int count = 0;
    if (isParallel) {
      scan.setParallel(true);
    }
    scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes
        .toBytes(columnName), CompareOp.LESS, Bytes.toBytes(remainder)));
    ResultScanner scanner = table.getScanner(scan);
    Result r = scanner.next();
    while (r != null) {
      count++;
      r = scanner.next();
    }
    System.out.println("++ Scanning finished with count : " + count + " ++");
    scanner.close();
    table.close();
  }
}
