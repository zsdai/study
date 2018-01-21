package com.demo.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTest {
	HBaseAdmin admin = null;
	Configuration conf = null;

	/**
	 * 构造函数初始化加载
	 */
	public HBaseTest() {
		try {
			// System.setProperty("hadoop.home.dir", "/application/hadoop");
			conf = new Configuration();
			conf.set("hbase.zookeeper.quorum", "192.168.200.100:2181");
			conf.set("hbase.rootdir", "hdfs://192.168.200.100:9000/hbase");
			admin = new HBaseAdmin(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		HBaseTest hBaseTest = new HBaseTest();
		// hBaseTest.createTable("javatest1", "cf");
		// hBaseTest.deleteTable("javatest1");
		// hBaseTest.getALLTables();
		//hBaseTest.addOneRecord("javatest", "key1", "cf", "age", "24");
		hBaseTest.getKey("javatest", "key1");
		//hBaseTest.getResultScanner("javatest");
		 //hBaseTest.getResultByColumn("javatest", "key1", "cf", "name");
		 //hBaseTest.deleteColumn("javatest", "key", "cf", "age");
		 //hBaseTest.deleteAllColumn("javatest", "key2");
		System.out.println("执行完毕...");

	}
    /**
     * 删除某一行键下的所有列
     * @param tableName 表名
     * @param rowKey  行键
     * @throws Exception
     */
    public void deleteAllColumn(String tableName,String rowKey) throws Exception{
        HTable hTable=new HTable(conf, tableName);
        Delete delete=new Delete(Bytes.toBytes(rowKey));
        hTable.delete(delete);
        System.out.println("all columns deleted!");
    }
    /**
     * 删除某一列数据
     * @param tableName 表名
     * @param rowKey 行键
     * @param familyName  列族
     * @param columnName  列名
    * @throws Exception 
     */
    public void deleteColumn(String tableName,String rowKey,
                        String familyName,String columnName) throws Exception{
        HTable hTable=new HTable(conf, tableName);
        Delete delete=new Delete(Bytes.toBytes(rowKey));
        delete.deleteColumn(Bytes.toBytes(familyName), 
                              Bytes.toBytes(columnName));
       hTable.delete(delete);
       System.out.println(columnName+" deleted!");
    }
	 /**
     * 读取某一列的数据
     * @param tableName 表名
     * @param rowKey 行键
     * @param falimyName 列族名
     * @param ColumnName 列名
     * @throws Exception
     */
    public void getResultByColumn(String tableName,String rowKey,
            String falimyName,String columnName) throws Exception{
        HTable hTable=new HTable(conf, tableName);
        Get get=new Get(Bytes.toBytes(rowKey));
        //指明列族与列
        get.addColumn(Bytes.toBytes(falimyName), Bytes.toBytes(columnName));
        Result result = hTable.get(get);
        List<KeyValue> list = result.list();
        for (KeyValue keyValue : list) {
           System.out.print(" Family:"+
                      Bytes.toString(keyValue.getFamily()));
           System.out.print(" Column:"+
                          Bytes.toString(keyValue.getQualifier()));            
           System.out.print(" Value:"+
                          Bytes.toString(keyValue.getValue()));
           System.out.print(" TimeStamp:"+keyValue.getTimestamp());
           System.out.println(" \n===================================");
       }
    }
	/**
     * 根据star_rowKey、stop_rowKey扫描Hbase表
     * @param tableName 表名
     * @param start_rowkey 开始行键
     * @param stop_rowkey  结束行键
     * @throws IOException
     * TODO 没有测试
     */
     public void getResultScanner(String tableName, 
                String start_rowkey,String stop_rowkey) throws IOException {
            Scan scan = new Scan();
            //注意下面两行代码
            scan.setStartRow(Bytes.toBytes(start_rowkey));
            scan.setStopRow(Bytes.toBytes(stop_rowkey));
            ResultScanner rs = null;
            HTable table = new HTable(conf, Bytes.toBytes(tableName));
            try {
                rs = table.getScanner(scan);
                for (Result r : rs) {
                    for (KeyValue kv : r.list()) {
                        System.out.println("row:" + Bytes.toString(kv.getRow()));
                        System.out.println("family:"
                                + Bytes.toString(kv.getFamily()));
                        System.out.println("qualifier:"
                                + Bytes.toString(kv.getQualifier()));
                        System.out
                                .println("value:" + Bytes.toString(kv.getValue()));
                        System.out.println("timestamp:" + kv.getTimestamp());
                        System.out
                                .println("-------------------------------------------");
                    }
                }
            } finally {
                rs.close();
            }
        }
    /**
     * Scan扫描整张表
     * @param tableName 表名
     * @throws Exception 
     */
    public void getResultScanner(String tableName) throws Exception{
        HTable hTable=new HTable(conf, tableName);
        Scan scan=new Scan();
        ResultScanner rs=null;
        try{
            rs=hTable.getScanner(scan);
            for (Result result : rs) {
                List<KeyValue> list = result.list();
                for (KeyValue keyValue : list) {
                    System.out.print("RowKey:"+
                                   Bytes.toString(keyValue.getRow()));
                    System.out.print(" Family:"+
                                   Bytes.toString(keyValue.getFamily()));
                    System.out.print(" Column:"+
                                   Bytes.toString(keyValue.getQualifier()));            
                    System.out.print(" Value:"+
                                   Bytes.toString(keyValue.getValue()));
                    System.out.print(" TimeStamp:"+keyValue.getTimestamp());
                    System.out.println(" \n===================================");
                }
            }
        }finally{
            rs.close();
        }
    }

	/**
	 * 查询一条记录
	 * 
	 * @param tableName
	 * @param rowKey
	 */
	public void getKey(String tableName, String rowKey) {
		HTablePool hTable = new HTablePool(conf, 1000);
		HTableInterface table = hTable.getTable(tableName);
		Get get = new Get(rowKey.getBytes());
		try {
			Result rs = table.get(get);
			if (rs.raw().length == 0) {
				System.out.println("不存在关键字为 " + rowKey + " 的行！");
			} else {
				for (KeyValue kv : rs.raw()) {
					System.out.println(new String(kv.getKey()) + "\t" + new String(kv.getValue()));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 往表中添加一条记录
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param column
	 * @param value
	 */
	public boolean addOneRecord(String tableName, String rowKey, String family, String column, String value) {
		HTablePool hTable = new HTablePool(conf, 1000);
		HTableInterface table = hTable.getTable(tableName);
		Put put = new Put(rowKey.getBytes());
		put.add(family.getBytes(), column.getBytes(), value.getBytes());
		try {
			table.put(put);
			System.out.println("添加记录 " + rowKey + " 成功！");
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("添加记录 " + rowKey + " 异常！" + e.getMessage());
			return false;
		}
	}

	public void createTable(String tableName, String family) throws IOException {
		if (admin.tableExists(tableName)) {
			System.out.println(tableName + "表已经存在");
		} else {
			HTableDescriptor descriptor = new HTableDescriptor(tableName);
			descriptor.addFamily(new HColumnDescriptor(family));
			admin.createTable(descriptor);
			System.out.println(tableName + "创建成功");
		}
	}

	public void deleteTable(String tableName) throws Exception {

		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println("table deleted!");
		}
	}

	/**
	 * 查询所有的列表
	 * 
	 * @throws Exception
	 */
	public List getALLTables() throws Exception {
		List<String> tables = null;
		if (admin != null) {
			HTableDescriptor[] listTables = admin.listTables();
			if (listTables.length > 0) {
				tables = new ArrayList<String>();
				for (HTableDescriptor tableDesc : listTables) {
					tables.add(tableDesc.getNameAsString());
					System.out.println(tableDesc.getNameAsString());
				}
			}
		}
		return tables;
	}

}
