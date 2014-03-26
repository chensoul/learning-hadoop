package com.embracesource.jmeter.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ServerCallable;

public class HTableUtil {
  @SuppressWarnings({ "deprecation" })
  public static void putAndCommit(HTable table, final Put put) throws IOException {
    try {
      table.getConnection().getRegionServerWithRetries(
        new ServerCallable<Boolean>(table.getConnection(), table.getTableName(), put.getRow()) {
          public Boolean call() throws Exception {
            server.put(location.getRegionInfo().getRegionName(), put);
            return true;
          }
        });
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
