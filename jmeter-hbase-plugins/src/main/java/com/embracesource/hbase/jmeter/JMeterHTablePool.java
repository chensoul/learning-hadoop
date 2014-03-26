package com.embracesource.jmeter.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

/**
 * 双重检查加锁 单例模式
 * 
 * @author allen
 * 
 */
public class JMeterHTablePool {
  public static HTablePool tablePool = null;

  private static JMeterHTablePool instancePool;

  private JMeterHTablePool(Configuration config, int poolsize, byte[] tableName,
      final boolean autoFlush, final long writeBufferSize) {
    tablePool = new HTablePool(config, poolsize, new HTableFactory() {
      @Override
      public HTableInterface createHTableInterface(Configuration config, byte[] tableName) {
        try {
          HTable hTable = new HTable(config, tableName);
          hTable.setAutoFlush(autoFlush);
          hTable.setWriteBufferSize(writeBufferSize);
          return hTable;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void releaseHTableInterface(HTableInterface table) throws IOException {
        table.flushCommits();
        super.releaseHTableInterface(table);
      }
    });
  }

  public static JMeterHTablePool getinstancePool(Configuration config, int poolSize,
      byte[] tableName, final boolean autoFlush, final long writeBufferSize) {
    if (instancePool == null) {
      synchronized (JMeterHTablePool.class) {
        if (instancePool == null) {
          System.out.println("Pool instance");
          instancePool =
              new JMeterHTablePool(config, poolSize, tableName, autoFlush, writeBufferSize);
        }
      }
    }
    return instancePool;
  }

  @SuppressWarnings("deprecation")
  public synchronized void flush(String tableName) throws IOException {
    HTableInterface hTable = tablePool.getTable(tableName);
    try {
      hTable.flushCommits();
    } finally {
      if (hTable != null) {
        tablePool.putTable(hTable);
      }
    }
  }

  public synchronized void close() throws IOException {
    tablePool.close();
  }

  public synchronized void close(String tableName) throws IOException {
    tablePool.closeTablePool(tableName.getBytes());
  }

}