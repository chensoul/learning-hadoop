package com.embracesource.jmeter.hbase;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

public class HbaseJMeter extends AbstractJavaSamplerClient {
  private HTableInterface table;

  private static Configuration conf = null;

  private Put put = null;

  private Get get = null;

  private String methedType = null;

  private int keyNumLength = 0;

  private String[] cfs = null;

  private String[] qualifiers = null;

  private String values = null;

  private boolean writeToWAL = true;

  private boolean keyRondom = true;

  @Override
  public void setupTest(JavaSamplerContext context) {
    super.setupTest(context);
    String hbaseZK = context.getParameter("hbase.zookeeper.quorum");
    conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", hbaseZK);
    conf.set("hbase.ipc.client.tcpnodelay", "true");
    conf.set("hbase.client.pause", "20");
    conf.set("ipc.ping.interval", "3000");
    conf.set("hbase.client.retries.number", "11");

    if (table == null) {
      String tableName = context.getParameter("tableName");
      byte[] tableNamebyte = tableName.getBytes();
      boolean autoFlush = Boolean.valueOf(context.getParameter("autoFlush"));
      long writeBufferSize = Long.valueOf(context.getParameter("writeBufferSize"));
      int poolSize = Integer.parseInt(context.getParameter("poolSize"));
      try {
        table =
            JMeterHTablePool.getinstancePool(conf, poolSize, tableNamebyte, autoFlush,
              writeBufferSize).tablePool.getTable(tableName);
      } catch (Exception e) {
        System.out.println("htable pool error");
      }
    }
    if (methedType == null) {
      methedType = context.getParameter("putOrget");
    }
    if (keyNumLength == 0) {
      keyNumLength = Integer.parseInt(context.getParameter("keyNumLength"));
    }
    if (cfs == null) {
      String cf = context.getParameter("cf");
      cfs = cf.split(",");
    }
    if (qualifiers == null) {
      String qualifier = context.getParameter("qualifier");
      qualifiers = qualifier.split(",");
    }
    if (values == null) {
      String valueLength = context.getParameter("valueLength");
      values = Strings.repeat('v', Integer.parseInt(valueLength));
    }
    if (writeToWAL == true) {
      writeToWAL = Boolean.valueOf(context.getParameter("writeToWAL"));
    }

    if (keyRondom == true) {
      keyRondom = Boolean.valueOf(context.getParameter("keyRondom"));
    }
  }

  public SampleResult runTest(JavaSamplerContext context) {
    SampleResult sampleResult = new SampleResult();
    sampleResult.sampleStart();
    String key = null;
    if (keyRondom) {
      key = String.valueOf(String.valueOf(new Random().nextInt(keyNumLength)).hashCode());
    } else {
      key = SequenceKey.getsequenceKey();
    }

    try {
      if (methedType.equals("put")) {
        put = new Put(Bytes.toBytes(key));
        put.setWriteToWAL(writeToWAL);
        for (int j = 0; j < cfs.length; j++) {
          for (int n = 0; n < qualifiers.length; n++) {
            put.add(Bytes.toBytes(cfs[j]), Bytes.toBytes(qualifiers[n]), Bytes.toBytes(values));
          }
        }
        table.put(put);
      } else if (methedType.equals("get")) {
        get = new Get((key).getBytes());
        table.get(get);
      }
      sampleResult.setSuccessful(true);
    } catch (Throwable e) {
      sampleResult.setSuccessful(false);
    } finally {
      sampleResult.sampleEnd();
    }
    return sampleResult;
  }

  @Override
  public Arguments getDefaultParameters() {
    Arguments params = new Arguments();

    params.addArgument("tableName", "test");
    params.addArgument("cf", "cf");
    params.addArgument("qualifier", "a");

    params.addArgument("putOrget", "put");
    params.addArgument("keyNumLength", "10000000");
    params.addArgument("keyRondom", "true");
    params.addArgument("valueLength", "1000");

    params.addArgument("autoFlush", "false");
    params.addArgument("writeBufferSize", "2097152");
    params.addArgument("writeToWAL", "true");
    params.addArgument("poolSize", "500");
    params
        .addArgument(
          "hbase.zookeeper.quorum",
          "tkpcjk01-12,tkpcjk01-13,tkpcjk01-14,tkpcjk01-15,tkpcjk01-16,tkpcjk01-17,tkpcjk01-18,tkpcjk01-19,tkpcjk01-20,tkpcjk01-21,tkpcjk01-22,tkpcjk01-23,tkpcjk01-24");
    return params;
  }

  @Override
  public void teardownTest(JavaSamplerContext context) {
    super.teardownTest(context);
    try {
      if (table != null) {
        table.flushCommits();
        table.close();
        table = null;
      }
    } catch (IOException e) {
      System.out.println("teardown error");
    }
  }
}