package com.embracesource.edh.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.embracesource.edh.hbase.table.create.Configure;

public class ReplicationTest {

  // private static String tableName = "blob1";
  private static String tableName = "rep6";

  public static void main(String[] agrs) {
    HBaseAdmin hba = null;
    HTable table = null;
    try {
      hba = new HBaseAdmin(Configure.getHBaseConfig());
      hba.createTable(Configure.genHTableDescriptor(tableName, (short) 4));
      table = new HTable(Configure.getHBaseConfig(), tableName);
      table.setWriteBufferSize(11);

      for (int i = 1; i < 60000; i++) {
        Put put = new Put(Bytes.toBytes("a" + i));

        put.add(
            Bytes.toBytes(Configure.FAMILY_NAME),
            Bytes.toBytes("key"),
            Bytes
                .toBytes(i
                    + "AAAAAAAAAABBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"));
        table.put(put);
      }

      table.flushCommits();

    } catch (MasterNotRunningException e) {
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (hba != null) {
        try {
          hba.close();
        } catch (IOException e) {
        }
      }
      if (table != null) {
        try {
          table.close();
        } catch (IOException e) {
        }
      }

    }
  }
}
