package com.embracesource.edh.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.GroupByClient;
import org.apache.hadoop.hbase.expression.Expression;
import org.apache.hadoop.hbase.expression.ExpressionFactory;
import org.apache.hadoop.hbase.expression.GroupByAggregationExpression;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.util.Bytes;

public class TestGroupby {
	private static GroupByClient groupByClient;
	private static byte[] table=null;

	void setup() {
		Configuration conf = HBaseConfiguration.create();
		groupByClient = new GroupByClient(conf);
		table = Bytes.toBytes("t");
	}

	public static void groupByCount() {
		List<Expression> groupByExpresstions = new ArrayList<Expression>();
		List<Expression> selectExpresstions = new ArrayList<Expression>();

		groupByExpresstions.add(ExpressionFactory.columnValue("f", "name"));
		selectExpresstions.add(ExpressionFactory.groupByKey(ExpressionFactory
				.columnValue("f", "name")));
		selectExpresstions.add(ExpressionFactory.count(ExpressionFactory
				.columnValue("f", "name")));
		groupBy(selectExpresstions, groupByExpresstions);
	}

	public static void groupBySum() {
		List<Expression> groupByExpresstions = new ArrayList<Expression>();
		List<Expression> selectExpresstions = new ArrayList<Expression>();

		groupByExpresstions.add(ExpressionFactory.columnValue("f", "name"));
		selectExpresstions.add(ExpressionFactory.groupByKey(ExpressionFactory
				.columnValue("f", "name")));
		selectExpresstions.add(ExpressionFactory.sum(ExpressionFactory
				.columnValue("f", "name")));
		groupBy(selectExpresstions, groupByExpresstions);
	}

	public static void distinct() {
		Scan[] scans = { new Scan() };

		List<Expression> selectExpresstions = new ArrayList<Expression>();
		selectExpresstions.add(ExpressionFactory.toLong(ExpressionFactory
				.columnValue("f", "name")));

		try {
			List<EvaluationResult[]> result = groupByClient.distinct(table,
					scans, selectExpresstions,
					GroupByAggregationExpression.AggregationType.COUNT);

		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	private static void groupBy(List<Expression> selectExpresstions,
			List<Expression> groupByExpresstions) {
		Scan[] scans = { new Scan() };

		try {
			List<EvaluationResult[]> result = groupByClient.groupBy(table,
					scans, groupByExpresstions, selectExpresstions, null);

			System.out.println("-- Output groupby result START --");

			for (EvaluationResult[] res : result) {
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
			System.out.println("-- Output groupby result END --");

		} catch (Throwable e) {
			e.printStackTrace();
		}

	}

}
