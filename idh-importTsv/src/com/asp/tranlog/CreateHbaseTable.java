package com.asp.tranlog;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class CreateHbaseTable {
	public static Configuration configuration = Global.configuration;

	public static void createTable(String tableName, String[] colNames) {
		System.out.println("start create table:" + tableName + "......");
		HBaseAdmin hBaseAdmin = null;
		try {
			hBaseAdmin = new HBaseAdmin(configuration);
			if (hBaseAdmin.tableExists(tableName)) {
				hBaseAdmin.disableTable(tableName);
				hBaseAdmin.deleteTable(tableName);
				System.out.println(tableName + " is exist,delete....");
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			if (colNames.length == 0) {
				System.out.println("colNames's length is 0");
			} else {
				for (int i = 0; i < colNames.length; i++) {
					tableDescriptor
							.addFamily(new HColumnDescriptor(colNames[i]));
				}
				hBaseAdmin.createTable(tableDescriptor);
			}
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				hBaseAdmin.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("end create table ......");
	}

}
