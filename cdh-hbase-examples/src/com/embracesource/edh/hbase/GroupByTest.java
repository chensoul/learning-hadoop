package com.embracesource.edh.hbase;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.GroupByClient;
import org.apache.hadoop.hbase.expression.Expression;
import org.apache.hadoop.hbase.expression.ExpressionFactory;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.util.Bytes;

public class GroupByTest {

  public static void main(String[] args) {
    Configuration conf = HBaseConfiguration.create();
    conf.setInt("hbase.client.retries.number", 1);
    conf.setInt("ipc.client.connect.max.retries", 1);
    
    byte[] table = Bytes.toBytes("t");

    try {
      GroupByClient groupByClient = new GroupByClient(conf);
      Scan[] scans = { new Scan() };

      List<Expression> groupByExpresstions = new ArrayList<Expression>();
      List<Expression> selectExpresstions = new ArrayList<Expression>();
      
      groupByExpresstions.add(ExpressionFactory.columnValue(
          "f", "id"));
      selectExpresstions.add(ExpressionFactory
          .groupByKey(ExpressionFactory.columnValue(
                  "f", "id")));
      
      List<EvaluationResult[]> groupByResultList = groupByClient
          .groupBy(table, scans, groupByExpresstions,
                  selectExpresstions, null);
      
      for (EvaluationResult[] res : groupByResultList) {
        String resultString = "";
        for (int i = 0; i < res.length; i++) {
            EvaluationResult er = res[i];
            resultString += "\t\t" + er.toString();
            if (0 == ((i + 1) % selectExpresstions.size())) {
                System.out.println(resultString);
                resultString = "";
            }
        }
    }

    } catch (Throwable e) {
      e.printStackTrace();
    }
  }
}
