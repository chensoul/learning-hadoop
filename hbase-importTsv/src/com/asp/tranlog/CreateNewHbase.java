package com.asp.tranlog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateNewHbase {

	/**
	 * @param args
	 */
	public static Configuration conf = Global.configuration;
	public static void createTable(String tabName)throws Exception{
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tabName)) {
		System.out.println(tabName + " exists!");
		admin.close();
		return;
		}
		HTableDescriptor table = new HTableDescriptor(tabName);
		table.addFamily(new HColumnDescriptor("f1"));
		table.addFamily(new HColumnDescriptor("f2"));
		table.addFamily(new HColumnDescriptor("f3"));
		table.getFamily(Bytes.toBytes("f1"));
		admin.createTable(table);
		admin.close();
	}
	public static void main(String[] args) throws Exception {
	
		createTable("test");
	}

}
