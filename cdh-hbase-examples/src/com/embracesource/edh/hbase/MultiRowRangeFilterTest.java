package com.embracesource.edh.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowKeyRange;
import org.apache.hadoop.hbase.util.Bytes;


public class MultiRowRangeFilterTest {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new Exception("Table name not specified.");
        }
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, args[0]);

        Scan scan = new Scan();
        List<RowKeyRange> ranges = new ArrayList<RowKeyRange>();
        ranges.add(new RowKeyRange(Bytes.toBytes("001"), Bytes.toBytes("002")));
        ranges.add(new RowKeyRange(Bytes.toBytes("003"), Bytes.toBytes("004")));
        ranges.add(new RowKeyRange(Bytes.toBytes("005"), Bytes.toBytes("006")));
        Filter filter = new MultiRowRangeFilter(ranges);
        scan.setFilter(filter);
        int count = 0;
        ResultScanner scanner = table.getScanner(scan);
        Result r = scanner.next();
        while (r != null) {
            count++;
            r = scanner.next();
        }
        System.out
                .println("++ Scanning finished with count : " + count + " ++");
        scanner.close();

    }

}
