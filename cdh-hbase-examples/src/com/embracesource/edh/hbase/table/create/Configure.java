package com.embracesource.edh.hbase.table.create;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;

/**
 * @description HBase相关配置参数
 */
public class Configure {

  public static final String FAMILY_NAME = "family";

  public static final Algorithm COMPRESS_TYPE = Algorithm.SNAPPY;

  // TTL(TTL Time To Live 版本存在时间，默认是forever)
  public static final boolean USE_TTL = false;

  // 会自动去读取classpath下的hbase-site.xml和hbase-default.xml文件，如果没有则需要手动通过创建configuration
  // 对象的通过set方法设置
  private static Configuration _config = HBaseConfiguration.create();

  public static Configuration getHBaseConfig() throws IOException {
    return _config;
  }

  public static void configHTable(HTableDescriptor ht) {

  }

  // HColumnDescriptor 代表的是column的schema
  public static void configColumnFamily(HColumnDescriptor desc) {
    desc.setMaxVersions(1);
    // 设置使用的过滤器的类型---
    // setBloomFilter:指定是否使用BloomFilter,可提高随机查询效率。默认关闭
    desc.setBloomFilterType(BloomType.ROW);
    // 设定数据压缩类型。默认无压缩
    desc.setCompressionType(COMPRESS_TYPE);
  }

  public static HTableDescriptor genHTableDescriptor(String tableName) {
    return genHTableDescriptor(tableName, Short.MIN_VALUE);
  }

  public static HTableDescriptor genHTableDescriptor(String tableName, short replica) {
    HTableDescriptor ht = new HTableDescriptor(tableName);
    HColumnDescriptor desc = new HColumnDescriptor(FAMILY_NAME);
    if (replica != Short.MIN_VALUE) {
      desc.setReplication(replica);
      System.out.println("genHTableDescriptor(String,short):replica---"
          + replica);
    }
    // desc.setLobStoreEnabled(true);
    ht.addFamily(desc);
    return ht;
  }

  public static HTableDescriptor genHTableDescriptor(String tableName, short replica, boolean lobenable) {
    HTableDescriptor ht = new HTableDescriptor(tableName);
    HColumnDescriptor desc = new HColumnDescriptor(FAMILY_NAME);
    if (replica != Short.MIN_VALUE) {
      desc.setReplication(replica);
      System.out.println("genHTableDescriptor(String,short):replica---"
          + replica);
    }
    desc.setBlbStoreEnabled(true);
    ht.addFamily(desc);
    return ht;
  }
}
