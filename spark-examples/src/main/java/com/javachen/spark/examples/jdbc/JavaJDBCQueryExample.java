package com.javachen.spark.examples.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.JdbcRDD;

public class JavaJDBCQueryExample {
	public static void main(String args[]) throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {

		String master = "local[2]";
		Class.forName("org.postgresql.Driver").newInstance();
		
		JavaSparkContext jsc = new JavaSparkContext(master,
				"JavaJDBCQueryExample");
		
		JavaRDD<String> jdbcRdd = JdbcRDD.create(jsc, new JdbcRDD.ConnectionFactory(){
			public Connection getConnection() throws Exception {
				return DriverManager.getConnection(
						"jdbc:postgresql://127.0.0.1:5432/postgres", "postgres",
						"postgres");
			}
		}, "select * from test offset ? limit ?", 0, 10, 2, new Function<ResultSet, String>() {
			public String call(ResultSet v1) throws Exception {
				return v1.getString(1)+":"+v1.getString(2);
			}
			
		});
		jdbcRdd.foreach(new VoidFunction<String>(){
			public void call(String t) throws Exception {
				System.out.println(Thread.currentThread().getId()+":"+t);
			}
			
		});
	}

}
