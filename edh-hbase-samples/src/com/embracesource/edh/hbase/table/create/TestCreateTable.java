package com.embracesource.edh.hbase.table.create;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class TestCreateTable {

  private static HBaseAdmin hba;

  public static void main(String[] args) {
    try {
      boolean isPartitionEnabled = false;

      hba = new HBaseAdmin(Configure.getHBaseConfig());
      createTable("Test_Table");
      createTableWithSplitKeys("Test_Table_SpilitKey");
      createTableWithStartAndEndKey("Test_Table_StartKey_EndKey_Num");

      // Async methods
      // Not finished now
      // createPartitionTableAsync("Test_Table_Async_Locator", new
      // SuffixClusterLocator());
      // createPartitionTableAsyncWithSpiltKeys("Test_Table_Async_SplitKeys_Locator",
      // new SuffixClusterLocator());

      try {
        tableExistFamily(hba, "Test_Table");
        if (isPartitionEnabled == true) {
          tableExistFamily(hba, "Test_Table_Locator");
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    } catch (MasterNotRunningException e) {
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      if (hba != null) {
        try {
          hba.close();
        } catch (IOException e) {
        }
      }

    }
  }

  public static void tableExistFamily(HBaseAdmin hba, String tableName) throws IOException {
    String testColumn1 = "test1";
    String testColumn2 = "test2";
    boolean before = false;
    boolean after = false;
    before = existsFamilyName(hba, tableName, testColumn1);
    boolean getException = false;
    try {
      hba.addColumn(tableName, new HColumnDescriptor(testColumn1));
    } catch (TableNotDisabledException e) {
      getException = true;
    } finally {
    }
    after = existsFamilyName(hba, tableName, testColumn1);

    getException = false;
    try {
      hba.deleteColumn(tableName, testColumn1);
    } catch (TableNotDisabledException e) {
      getException = true;
    } finally {
    }

    after = existsFamilyName(hba, tableName, testColumn1);

    hba.disableTable(tableName);

    before = existsFamilyName(hba, tableName, testColumn2);
    hba.addColumn(tableName, new HColumnDescriptor(testColumn2));
    after = existsFamilyName(hba, tableName, testColumn2);
    System.out.println(before + " : " + after);

    before = after;
    hba.deleteColumn(tableName, testColumn2);
    after = existsFamilyName(hba, tableName, testColumn2);
    System.out.println(before + " : " + after);

    hba.enableTable(tableName);
  }

  public static boolean existsFamilyName(HBaseAdmin hba, String tableName, String columnName) {
    HTableDescriptor[] list;
    try {
      list = hba.listTables();
      for (int i = 0; i < list.length; i++) {
        if (list[i].getNameAsString().equals(tableName))
          for (HColumnDescriptor hc : list[i].getColumnFamilies()) {
            if (hc.getNameAsString().equals(columnName))
              return true;
          }
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return false;
  }

  public static HTableDescriptor genHTableDescriptor(String tableName) {

    HTableDescriptor ht = new HTableDescriptor(tableName);
    HColumnDescriptor desc = new HColumnDescriptor(Configure.FAMILY_NAME);
    Configure.configColumnFamily(desc);
    ht.addFamily(desc);
    return ht;
  }

  public static byte[][] genSplitKeys() {
    return new byte[0][];
  }

  private static void removeTable(String tableName) throws IOException {
    if (hba.tableExists(tableName)) {
      hba.disableTable(tableName);
      hba.deleteTable(tableName);
    }
  }

  public static void createTable(String tableName) {

    boolean result = false;

    try {
      removeTable(tableName);
      hba.createTable(genHTableDescriptor(tableName));
      result = hba.tableExists(tableName);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
    }
  }

  public static void createTableWithSplitKeys(String tableName) {

    boolean result = false;
    try {
      removeTable(tableName);
      hba.createTable(genHTableDescriptor(tableName), genSplitKeys());
      result = hba.tableExists(tableName);

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void createTableWithStartAndEndKey(String tableName) {
    boolean result = false;
    try {
      removeTable(tableName);
      hba.createTable(genHTableDescriptor(tableName), Bytes.toBytes("123"),
          Bytes.toBytes("456"), 10);
      result = hba.tableExists(tableName);

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
