package com.embracesource.edh.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.expression.Expression;
import org.apache.hadoop.hbase.expression.ExpressionFactory;
import org.apache.hadoop.hbase.filter.ExpressionFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class ExpressionFilterTest {

    public static void main(String[] args) throws Exception {
    	
        if (args.length < 2) {
            throw new Exception("Table name not specified.");
        }
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, args[0]);
        String startKey = args[1];
        
        Expression exp = ExpressionFactory.eq(ExpressionFactory
                .toLong(ExpressionFactory.toString(ExpressionFactory
                        .columnValue("family", "longStr2"))), ExpressionFactory
                .constant(Long.parseLong("99")));
        ExpressionFilter expressionFilter = new ExpressionFilter(exp);
        Scan scan = new Scan(Bytes.toBytes(startKey), expressionFilter);
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
