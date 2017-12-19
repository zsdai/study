package com.demo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HbaseNewAPI {
	static Configuration conf = null;
	static Connection connection = null;
	static {
		try {
			Configuration conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", "192.168.200.100");
			connection = ConnectionFactory.createConnection(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * 创建表
	 * 
	 * @tableName 表名
	 * 
	 * @family 列族列表
	 */
	public static void creatTable(String tableName, String[] family) throws Exception {
		Admin admin = connection.getAdmin();
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
		for (int i = 0; i < family.length; i++) {
			desc.addFamily(new HColumnDescriptor(family[i]));
		}
		if (admin.tableExists(TableName.valueOf(tableName))) {
			System.out.println("table Exists!");
			System.exit(0);
		} else {
			admin.createTable(desc);
			System.out.println("create table Success!");
		}
	}

	public static void main(String[] args) throws Exception {
		String tableName = "blog2";
		String[] family = { "article", "author" };
		creatTable(tableName, family);
	}
}
