package com.embracesource.hbase.jmeter;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

public class JMeterTest extends AbstractJavaSamplerClient {
	private HTableInterface table;
	private static Configuration conf = null;
	private Put put = null ;
	private Get get= null ; 
	/*测试的方法*/
	private String methedType = null ;
	/*key取值长度*/
	private int keyNumLength = 0 ;
	/*列族数组*/
	private String[] cfs = null ;
	/*列数组*/
	private String[] qualifiers = null ;
	/*value值*/
	private String values =null ;
	/*是否记录日志*/
	private  boolean writeToWAL = true ;
	/**
	 * 初始化配置
	 */
	static {
		conf = HBaseConfiguration.create();
	}
	@Override
	public void setupTest(JavaSamplerContext context) {
		super.setupTest(context);
		if (table == null) {
			String tableName = context.getParameter("tableName");
			byte[] tableNamebyte = tableName.getBytes();
			boolean autoFlush = Boolean.valueOf(context.getParameter("autoFlush"));
			long  writeBufferSize = Long.valueOf(context.getParameter("writeBufferSize"));
			int poolSize = Integer.parseInt(context.getParameter("poolSize"));
			try {
				table = JMeterHTablePool.getinstancePool(conf,poolSize,tableNamebyte,autoFlush,writeBufferSize).tablePool.getTable(tableName);
			} catch (Exception e) {
				System.out.println("htable pool error");
			}
		}
		if( methedType == null ){
			methedType = context.getParameter("putOrget");
		}
		if( keyNumLength == 0){
			keyNumLength = Integer.parseInt(context.getParameter("keyNumLength"));
		}
		if(cfs == null){
			String cf = context.getParameter("cf");
			cfs = cf.split(",");
		}
		if( qualifiers == null ){
			String qualifier = context.getParameter("qualifier");
			qualifiers = qualifier.split(",");
		}		
		if( values == null ){
			String valueLength = context.getParameter("valueLength");
			values = Strings.repeat('v', Integer.parseInt(valueLength));
		}
		if( writeToWAL == true ){
			writeToWAL = Boolean.valueOf(context.getParameter("writeToWAL"));
		}
	}

	public SampleResult runTest(JavaSamplerContext context) {
		SampleResult sampleResult = new SampleResult();
		sampleResult.sampleStart();
		String key = String.valueOf(String.valueOf(new Random().nextInt(keyNumLength)).hashCode());
		
		try {
			if (methedType.equals("put")) {
				put = new Put(Bytes.toBytes(key));
				put.setWriteToWAL(writeToWAL);
				for (int j = 0; j < cfs.length; j++) {
					for (int n = 0; n < qualifiers.length; n++) {
					put.add(Bytes.toBytes(cfs[j]),
							Bytes.toBytes(qualifiers[n]),
							Bytes.toBytes(values));
					table.put(put);
					}
				}
			} else if (methedType.equals("get")) {
				get = new Get((key ).getBytes());
				table.get(get);
//				Result rs = table.get(get);
			}
			sampleResult.setSuccessful(true);
		} catch (Throwable e) {
			sampleResult.setSuccessful(false);
		} finally {
			sampleResult.sampleEnd();
		}
		// // 返回是否处理成功
		return sampleResult;
	}

	@Override
	public Arguments getDefaultParameters() {
		Arguments params = new Arguments();
		params.addArgument("putOrget", "put");
		params.addArgument("keyNumLength", "5");
		params.addArgument("valueLength", "1000");
		params.addArgument("cf", "cf");
		params.addArgument("qualifier", "a");
		params.addArgument("tableName","test");
		params.addArgument("autoFlush","false");
		params.addArgument("writeBufferSize","2097152");
		params.addArgument("writeToWAL","true");
		params.addArgument("poolSize","500");
		return params;
	}

	@Override
	public void teardownTest(JavaSamplerContext context) {
		super.teardownTest(context);
		try {
			if (table != null) {
				table.flushCommits();
				table.close();
				table = null ;
			}
		} catch (IOException e) {
			System.out.println("teardown error");
		}
	}
	 public static void main(String[] args){
		 for(int i = 0 ; i<100;i++){
			 
			String key = null ;
				key = String.valueOf(String.valueOf(new Random().nextInt(1000000)).hashCode());
				System.out.println(key);
		 }
	 }
}
