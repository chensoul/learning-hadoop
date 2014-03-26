package com.embracesource.edh.hbase.table.create;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class OperateTableUtil {
	
	//private static ResultHTMLGenerater resultHTMLGenerater = new ResultHTMLGenerater();
	
	//创建表是通过HBaseAdmin对象来操作的。HBaseAdmin负责表的META信息处理
    private static HBaseAdmin admin = null;
    
    static{
    	try {
			admin = new HBaseAdmin(Configure.getHBaseConfig());
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
    }
    
    /**
	 *@description 创建表，如果存在则先删除此表
	 *@param tableName 表名 
	 */
	public static void createTable(String tableName) {
		 boolean result = false;
	        try {
	        	//删除表
	            removeTable(tableName);
	            admin.createTable(genHTableDescriptor(tableName));
	            result = admin.tableExists(tableName);
	        } catch (IOException e) {
	            e.printStackTrace();
	        } finally {
	        	System.out.println(tableName + "是否创建成功:" + result);
	        }
	}
	
	/**
	 *@description 根据表名删除一张表
	 *@param tableName  表名 
	 * @throws IOException 
	 */
	public static void removeTable(String tableName) throws IOException {
		//判断是否存在此张表
		boolean exists = admin.tableExists(tableName);
		if(exists){
			//先使此张表离线
			admin.disableTable(tableName);
			//删除表
			admin.deleteTable(tableName);
			System.out.println("删除了已经存在的原表:" + tableName);
		}
	}
	
	/**
	 *@description 根据表名创建表描述对象，同时设置列族的属性 
	 */
	public static HTableDescriptor genHTableDescriptor(String tableName) {
		//HTableDescriptor 代表的是表的schema
		HTableDescriptor ht = new HTableDescriptor(tableName);
		//HColumnDescriptor 代表的是column的schema
		HColumnDescriptor desc = new HColumnDescriptor(Configure.FAMILY_NAME);
		Configure.configColumnFamily(desc);
		ht.addFamily(desc);
		return ht;
	}
	 
	 public static void createTableWithSplitKeys(String tableName) {
	        boolean result = false;
	        try {
	            removeTable(tableName);
	            /**
	             * @description Creates a new table with an initial set of empty regions defined by the specified split keys. 
	             * 				The total number of regions created will be the number of split keys plus one. 
	             * 				Synchronous operation. Note : Avoid passing empty split key.
	             * @param desc - table descriptor for table
	             * @param splitKeys - array of split keys for the initial regions of the table
	             */
	            admin.createTable(genHTableDescriptor(tableName), genSplitKeys());
	            
	            result = admin.tableExists(tableName);
	        } catch (IOException e) {
	            e.printStackTrace();
	        } finally {
	        	System.out.println("createTableWithSplitKeys(String tableName):---" +result );
	        }
	  }
	 
	 public static byte[][] genSplitKeys() {
	     return new byte[0][];
	 }
}
