//package com.embracesource.jmeter.hbase;
//
//import java.io.IOException;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.HTableInterface;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.jmeter.config.Arguments;
//import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
//import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
//import org.apache.jmeter.samplers.SampleResult;
//
//public class HbaseJMeterPut extends AbstractJavaSamplerClient {
//  private static HTableInterface table;
//
//  private static Configuration conf = null;
//
//  private Put put = null;
//
//  private byte[] cf = null;
//
//  private String[] qualifiers = null;
//
//  private byte[] values = null;
//
//  private boolean writeToWAL = true;
//
//  @Override
//  public void setupTest(JavaSamplerContext context) {
//    super.setupTest(context);
//
//    conf = HBaseConfiguration.create();
//    conf.set("hbase.zookeeper.quorum", context.getParameter("hbase.zookeeper.quorum"));
//    conf.set("hbase.ipc.client.tcpnodelay", "true");
//    conf.set("hbase.client.pause", "20");
//    conf.set("ipc.ping.interval", "3000");
//    conf.set("hbase.client.retries.number", "4");
//
//    Long writeBufferSize = Long.valueOf(context.getParameter("writeBufferSize"));
//    Integer poolSize = Integer.parseInt(context.getParameter("poolSize"));
//    try {
//      table =
//          JMeterHTablePool.getinstancePool(conf, poolSize, writeBufferSize).tablePool
//              .getTable(context.getParameter("tableName"));
//    } catch (Exception e) {
//    }
//    cf = context.getParameter("cf").getBytes();
//    qualifiers = context.getParameter("qualifier").split(",");
//    writeToWAL = Boolean.valueOf(context.getParameter("writeToWAL"));
//    values = Bytes.toBytes("vvvvv");
//  }
//
//  public SampleResult runTest(JavaSamplerContext context) {
//    String key = SequenceKey.getsequenceKey();
//
//    SampleResult sampleResult = new SampleResult();
//    sampleResult.sampleStart();
//    try {
//      put = new Put(Bytes.toBytes(key));
//      put.setWriteToWAL(writeToWAL);
//      for (int n = 0; n < qualifiers.length; n++) {
//        put.add(cf, Bytes.toBytes(qualifiers[n]), values);
//      }
//
//      table.put(put);
//      sampleResult.setSuccessful(true);
//    } catch (Throwable e) {
//      sampleResult.setSuccessful(false);
//    } finally {
//      sampleResult.sampleEnd();
//    }
//    return sampleResult;
//  }
//
//  @Override
//  public Arguments getDefaultParameters() {
//    Arguments params = new Arguments();
//    params.addArgument("tableName", "test");
//    params.addArgument("cf", "f");
//    params.addArgument("qualifier", "a,b,c,d,e,f,g,h,i,j,g,k,l,m,n,o,p,q,r,s,t");
//    params.addArgument("writeBufferSize", "2097152");
//    params.addArgument("writeToWAL", "true");
//    params.addArgument("poolSize", "500");
//    params
//        .addArgument(
//          "hbase.zookeeper.quorum",
//          "tkpcjk01-12,tkpcjk01-13,tkpcjk01-14,tkpcjk01-15,tkpcjk01-16,tkpcjk01-17,tkpcjk01-18,tkpcjk01-19,tkpcjk01-20,tkpcjk01-21,tkpcjk01-22,tkpcjk01-23,tkpcjk01-24");
//    return params;
//  }
//
//  @Override
//  public void teardownTest(JavaSamplerContext context) {
//    super.teardownTest(context);
//    try {
//      table.close();
//    } catch (IOException e) {
//    }
//  }
//}