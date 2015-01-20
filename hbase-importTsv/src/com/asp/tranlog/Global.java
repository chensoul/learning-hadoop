package com.asp.tranlog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class Global {
	public final static String DRIVERNAME = "org.apache.hadoop.hive.jdbc.HiveDriver";

	public final static Configuration configuration;
	static {
		configuration = HBaseConfiguration.create();
	}
}
