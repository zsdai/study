package com.demo.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HBaseTest {
	HBaseAdmin admin = null;
	Configuration conf = null;

	/**
	 * 构造函数初始化加载
	 */
	public HBaseTest() {
		try {
			conf = new Configuration();
			conf.set("hbase.zookeeper.quorum", "192.168.200.100:2181");
			conf.set("hbase.rootdir", "hdfs://192.168.200.100:9000/hbase");
			admin = new HBaseAdmin(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public void createTable(String tableName,String family) throws IOException{
		if(admin.tableExists(tableName)){
			System.out.println(tableName+"表已经存在");
		}else{
			HTableDescriptor descriptor = new HTableDescriptor(tableName);
			descriptor.addFamily(new HColumnDescriptor(family));
			admin.createTable(descriptor);
			System.out.println(tableName+"创建成功");
		}
	}
	public static void main(String[] args) throws IOException {
		HBaseTest hBaseTest = new HBaseTest();
		hBaseTest.createTable("javatest", "cf");
	}
}
