package com.embracesource.edh.hbase.table.create;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;

public class TableBuilder {
  /**
   * @param args
   */
  public static void main(String[] args) {
    Configuration conf = HBaseConfiguration.create();
    

    byte[] columnFamily = Bytes.toBytes("f");

    String tableName = "t";

    try {
      ZKUtil.applyClusterKeyToConf(conf, "edh1:2181:/hbase");
      HBaseAdmin hba = new HBaseAdmin(conf);
      if (hba.tableExists(tableName)) {
        hba.disableTable(tableName);
        hba.deleteTable(tableName);
      }
      HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
      HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamily);
      columnDescriptor.setMaxVersions(1);
      columnDescriptor.setBloomFilterType(BloomType.ROW);
      tableDescriptor.addFamily(columnDescriptor);
      hba.createTable(tableDescriptor);
      hba.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

}
