package com.demo.hbase;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hbase.*;  
import org.apache.hadoop.hbase.client.*;  
import org.apache.hadoop.hbase.util.Bytes;  
  
import java.io.IOException;  
  
public class Test {  
    // 声明静态配置  
    static Configuration conf = null;  
    static  Connection  connection = null;  
    static {  
        try{  
            Configuration conf = HBaseConfiguration.create();  
            conf.set("hbase.zookeeper.quorum", "hadoop-01,hadoop-02,hadoop-03");  
            connection = ConnectionFactory.createConnection(conf);  
        }catch (Exception e){  
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
    public static void creatTable(String tableName, String[] family)  
            throws Exception {  
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
  
    /* 
     * 为表添加数据（适合知道有多少列族的固定表） 
     * 
     * @rowKey rowKey 
     * 
     * @tableName 表名 
     * 
     * @column1 第一个列族列表 
     * 
     * @value1 第一个列的值的列表 
     * 
     * @column2 第二个列族列表 
     * 
     * @value2 第二个列的值的列表 
     */  
    public static void addData(String rowKey, String tableName,  
                               String[] column1, String[] value1, String[] column2, String[] value2)  
            throws IOException {  
        Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey  
        Table table = connection.getTable(TableName.valueOf(tableName));// HTabel负责跟记录相关的操作如增删改查等//  
        // 获取表  
        HColumnDescriptor[] columnFamilies = table.getTableDescriptor() // 获取所有的列族  
                .getColumnFamilies();  
  
        for (int i = 0; i < columnFamilies.length; i++) {  
            String familyName = columnFamilies[i].getNameAsString(); // 获取列族名  
            if (familyName.equals("article")) { // article列族put数据  
                for (int j = 0; j < column1.length; j++) {  
                    put.addColumn(Bytes.toBytes(familyName),  
                            Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));  
                }  
            }  
            if (familyName.equals("author")) { // author列族put数据  
                for (int j = 0; j < column2.length; j++) {  
                    put.addColumn(Bytes.toBytes(familyName),  
                            Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));  
                }  
            }  
        }  
        table.put(put);  
        System.out.println("add data Success!");  
    }  
  
    /* 
     * 根据rwokey查询 
     * 
     * @rowKey rowKey 
     * 
     * @tableName 表名 
     */  
    public static Result getResult(String tableName, String rowKey)  
            throws IOException {  
        Get get = new Get(Bytes.toBytes(rowKey));  
        Table table = connection.getTable(TableName.valueOf(tableName));// 获取表  
        Result result = table.get(get);  
        for (Cell cell : result.listCells()) {  
            System.out.println("family:" + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));  
            System.out.println("qualifier:" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));  
            System.out.println("value:" + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));  
            System.out.println("Timestamp:" + cell.getTimestamp());  
            System.out.println("-------------------------------------------");  
        }  
        return result;  
    }  
  
    /* 
     * 遍历查询hbase表 
     * 
     * @tableName 表名 
     */  
    public static void getResultScann(String tableName) throws IOException {  
        Scan scan = new Scan();  
        ResultScanner rs = null;  
        Table table = connection.getTable(TableName.valueOf(tableName));  
        try {  
            rs = table.getScanner(scan);  
            for (Result r : rs) {  
                for (Cell cell : r.listCells()) {  
                    System.out.println("row:" + Bytes.toString(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength()));  
                    System.out.println("family:"  
                            + Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getValueLength()));  
                    System.out.println("qualifier:"  
                            + Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()));  
                    System.out  
                            .println("value:" + Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()));  
                    System.out.println("timestamp:" + cell.getTimestamp());  
                    System.out  
                            .println("-------------------------------------------");  
                }  
            }  
        } finally {  
            rs.close();  
        }  
    }  
  
    /* 
     * 遍历查询hbase表 
     * 
     * @tableName 表名 
     */  
    public static void getResultScann(String tableName, String start_rowkey,  
                                      String stop_rowkey) throws IOException {  
        Scan scan = new Scan();  
        scan.setStartRow(Bytes.toBytes(start_rowkey));  
        scan.setStopRow(Bytes.toBytes(stop_rowkey));  
        ResultScanner rs = null;  
        Table table = connection.getTable(TableName.valueOf(tableName));  
        try {  
            rs = table.getScanner(scan);  
            for (Result r : rs) {  
                for (Cell cell : r.listCells()) {  
                    System.out.println("row:" + Bytes.toString(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength()));  
                    System.out.println("family:"  
                            + Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getValueLength()));  
                    System.out.println("qualifier:"  
                            + Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()));  
                    System.out  
                            .println("value:" + Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()));  
                    System.out.println("timestamp:" + cell.getTimestamp());  
                    System.out  
                            .println("-------------------------------------------");  
                }  
            }  
        } finally {  
            rs.close();  
        }  
    }  
  
    /* 
     * 查询表中的某一列 
     * 
     * @tableName 表名 
     * 
     * @rowKey rowKey 
     */  
    public static void getResultByColumn(String tableName, String rowKey,  
                                         String familyName, String columnName) throws IOException {  
        Table table = connection.getTable(TableName.valueOf(tableName));  
        Get get = new Get(Bytes.toBytes(rowKey));  
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName)); // 获取指定列族和列修饰符对应的列  
        Result result = table.get(get);  
        for (Cell cell : result.listCells()) {  
            System.out.println("family:"  
                    + Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getValueLength()));  
            System.out.println("qualifier:"  
                    + Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()));  
            System.out  
                    .println("value:" + Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()));  
            System.out.println("timestamp:" + cell.getTimestamp());  
            System.out.println("-------------------------------------------");  
        }  
    }  
  
    /* 
     * 更新表中的某一列 
     * 
     * @tableName 表名 
     * 
     * @rowKey rowKey 
     * 
     * @familyName 列族名 
     * 
     * @columnName 列名 
     * 
     * @value 更新后的值 
     */  
    public static void updateTable(String tableName, String rowKey,  
                                   String familyName, String columnName, String value)  
            throws IOException {  
        Table table = connection.getTable(TableName.valueOf(tableName));  
        Put put = new Put(Bytes.toBytes(rowKey));  
        put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName),  
                Bytes.toBytes(value));  
        table.put(put);  
        System.out.println("update table Success!");  
    }  
  
    /* 
     * 查询某列数据的多个版本 
     * 
     * @tableName 表名 
     * 
     * @rowKey rowKey 
     * 
     * @familyName 列族名 
     * 
     * @columnName 列名 
     */  
    public static void getResultByVersion(String tableName, String rowKey,  
                                          String familyName, String columnName) throws IOException {  
        Table table = connection.getTable(TableName.valueOf(tableName));  
        Get get = new Get(Bytes.toBytes(rowKey));  
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));  
        get.setMaxVersions(5);  
        Result result = table.get(get);  
        for (KeyValue kv : result.list()) {  
            System.out.println("family:" + Bytes.toString(kv.getFamily()));  
            System.out  
                    .println("qualifier:" + Bytes.toString(kv.getQualifier()));  
            System.out.println("value:" + Bytes.toString(kv.getValue()));  
            System.out.println("Timestamp:" + kv.getTimestamp());  
            System.out.println("-------------------------------------------");  
        }  
        /* 
         * List<?> results = table.get(get).list(); Iterator<?> it = 
         * results.iterator(); while (it.hasNext()) { 
         * System.out.println(it.next().toString()); } 
         */  
    }  
  
    /* 
     * 删除指定的列 
     * 
     * @tableName 表名 
     * 
     * @rowKey rowKey 
     * 
     * @familyName 列族名 
     * 
     * @columnName 列名 
     */  
    public static void deleteColumn(String tableName, String rowKey,  
                                    String falilyName, String columnName) throws IOException {  
        Table table = connection.getTable(TableName.valueOf(tableName));  
        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));  
        deleteColumn.addColumn(Bytes.toBytes(falilyName),  
                Bytes.toBytes(columnName));  
        table.delete(deleteColumn);  
        System.out.println(falilyName + ":" + columnName + "is deleted!");  
    }  
  
    /* 
     * 删除指定的列 
     * 
     * @tableName 表名 
     * 
     * @rowKey rowKey 
     */  
    public static void deleteAllColumn(String tableName, String rowKey)  
            throws IOException {  
        Table table = connection.getTable(TableName.valueOf(tableName));  
        Delete deleteAll = new Delete(Bytes.toBytes(rowKey));  
        table.delete(deleteAll);  
        System.out.println("all columns are deleted!");  
    }  
  
    /* 
     * 删除表 
     * 
     * @tableName 表名 
     */  
    public static void deleteTable(String tableName) throws IOException {  
        Admin admin = connection.getAdmin();  
        admin.disableTable(TableName.valueOf(tableName));  
        admin.deleteTable(TableName.valueOf(tableName));  
        System.out.println(tableName + "is deleted!");  
    }  
  
    public static void main(String[] args) throws Exception {  
  
        // 创建表  
        String tableName = "blog2";  
        String[] family = { "article", "author" };  
         creatTable(tableName, family);  
  
        // 为表添加数据  
  
        String[] column1 = { "title", "content", "tag" };  
        String[] value1 = {  
                "Head First HBase",  
                "HBase is the Hadoop database. Use it when you need random, realtime read/write access to your Big Data.",  
                "Hadoop,HBase,NoSQL" };  
        String[] column2 = { "name", "nickname" };  
        String[] value2 = { "nicholas", "lee" };  
        addData("rowkey1", "blog2", column1, value1, column2, value2);  
        addData("rowkey2", "blog2", column1, value1, column2, value2);  
        addData("rowkey3", "blog2", column1, value1, column2, value2);  
  
        // 遍历查询  
        getResultScann("blog2", "rowkey4", "rowkey5");  
        // 根据row key范围遍历查询  
        getResultScann("blog2", "rowkey4", "rowkey5");  
  
        // 查询  
        getResult("blog2", "rowkey1");  
  
        // 查询某一列的值  
        getResultByColumn("blog2", "rowkey1", "author", "name");  
  
        // 更新列  
        updateTable("blog2", "rowkey1", "author", "name", "bin");  
  
        // 查询某一列的值  
        getResultByColumn("blog2", "rowkey1", "author", "name");  
  
        // 查询某列的多版本  
        getResultByVersion("blog2", "rowkey1", "author", "name");  
  
        // 删除一列  
        deleteColumn("blog2", "rowkey1", "author", "nickname");  
  
        // 删除所有列  
        deleteAllColumn("blog2", "rowkey1");  
  
        // 删除表  
        deleteTable("blog2");  
  
    }  
}  