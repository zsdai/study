package com.demo.hbase;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class DemoToHbase {
    static HBaseAdmin hBaseAdmin=null;
    public static void main(String[] args) throws Exception {
    
        // 创建表
        String tableName = "blog"; // 表名
    
        String[] family = { "artitle", "author" }; //列族名
//        createTable(tableName, family);
        
        // 添加数据
        String[] column1 = { "title", "content", "tag" };
        String[] value1 = { "Hadoop", "Hadoop is Number one", "big data" };
        String[] column2 = { "name", "nickname" };
        String[] value2 = { "Jarry", "oldJarry" };
//        addData(tableName, "rk1", column1, value1, column2, value2);
//        addData(tableName, "rk2", column1, value1, column2, value2);
//        addData(tableName, "rk3", column1, value1, column2, value2);
//        addData(tableName, "rk4", column1, value1, column2, value2);
        //根据rowKey查询数据
//        getResult(tableName,"rk2");    
        //扫描整张表
//        getResultScanner(tableName);
        
        //根据startRowKey和stopRowKey扫描表
        getResultScanner(tableName, "rk1", "rk4");
        
        //获取某一列的数据
//        getResultByColumn(tableName,"rk2","author","nickname");
        
        //删除某一列数据
//        deleteColumn(tableName, "rk4", "author", "nickname");
        
        //删除某一行键下的所有列
//        deleteAllColumn(tableName, "rk4");
        
        //删除整张表
//        deleteTable("user");
        
    }

    // 静态的Configuration
    static Configuration configuration = null;
    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "192.168.10.50");
        configuration.set("hbase.rootdir", "hdfs://192.168.10.50:9000/hbase");
        
    }

    /**
     * 创建表
     * 
     * @param tableName
     *            表名
     * @param family
     *            列族数组
     * @throws Exception
     */
    public static void createTable(String tableName, String[] family)
            throws Exception {
        
        hBaseAdmin = new HBaseAdmin(configuration);
        
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        
        // 遍历列族数组，并将其添加到hTableDescriptor中
        for (int i = 0; i < family.length; i++) {
            
            hTableDescriptor.addFamily(new HColumnDescriptor(family[i]));
            
        }
        // 判断表是否存在
        
        
            System.out.println(".....");
            hBaseAdmin.createTable(hTableDescriptor);
            System.out.println("table create success!");
        
    }

    /**
     * 添加数据
     * 
     * @param tableName
     *            表名
     * @param rowKey
     *            行键
     * @param column1
     *            列族1中的列
     * @param value1
     *            列族1中的列的值
     * @param column2
     *            列族2中的列
     * @param value2
     *            列族2中的列的值
     * @throws Exception
     */
    public static void addData(String tableName, String rowKey,
            String[] column1, String[] value1, String[] column2, String[] value2)
            throws Exception {

        @SuppressWarnings("resource")
        HTable hTable = new HTable(configuration, tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        // 获取表中的所有列族
        HColumnDescriptor[] columnFamilies = hTable.getTableDescriptor()
                .getColumnFamilies();

        for (int i = 0; i < columnFamilies.length; i++) {
            // 获取列族名字
            String familyName = columnFamilies[i].getNameAsString();
            if (familyName.equals("artitle")) {
                for (int j = 0; j < column1.length; j++) {
                    put.add(Bytes.toBytes(familyName), // 列族
                            Bytes.toBytes(column1[j]), // 列
                            Bytes.toBytes(value1[j])); // 列的值
                }
            }
            if (familyName.equals("author")) {
                for (int j = 0; j < column2.length; j++) {
                    put.add(Bytes.toBytes(familyName),
                            Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
                }
            }
        }
        hTable.put(put);
        System.out.println("add data success!");
    }

    /**
     * 根据rowKey查询
     * 
     * @param tableName
     *            表名
     * @param rowKey
     *            行键
     * @throws Exception
     */
    
    public static void getResult(String tableName, String rowKey)
            throws Exception {
        @SuppressWarnings("resource")
        HTable hTable = new HTable(configuration, Bytes.toBytes(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = hTable.get(get);
        List<KeyValue> list = result.list();
        for (KeyValue keyValue : list) {
            System.out.println("Family:" +
                          Bytes.toString(keyValue.getFamily()));
            System.out.println("Column:"+
                          Bytes.toString(keyValue.getQualifier()));        
            System.out.println("Value:"+
                         Bytes.toString(keyValue.getValue()));
            System.out.println("TimeStamp:"+keyValue.getTimestamp());
            
        }
    }
    /**
     * Scan扫描整张表
     * @param tableName 表名
     * @throws Exception 
     */
    public static void getResultScanner(String tableName) throws Exception{
        HTable hTable=new HTable(configuration, tableName);
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
     * 根据star_rowKey、stop_rowKey扫描Hbase表
     * @param tableName 表名
     * @param start_rowkey 开始行键
     * @param stop_rowkey  结束行键
     * @throws IOException
     */
     public static void getResultScanner(String tableName, 
                String start_rowkey,String stop_rowkey) throws IOException {
            Scan scan = new Scan();
            //注意下面两行代码
            scan.setStartRow(Bytes.toBytes(start_rowkey));
            scan.setStopRow(Bytes.toBytes(stop_rowkey));
            ResultScanner rs = null;
            HTable table = new HTable(configuration, Bytes.toBytes(tableName));
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
      * 读取某一列的数据
      * @param tableName 表名
      * @param rowKey 行键
      * @param falimyName 列族名
      * @param ColumnName 列名
      * @throws Exception
      */
     public static void getResultByColumn(String tableName,String rowKey,
             String falimyName,String columnName) throws Exception{
         HTable hTable=new HTable(configuration, tableName);
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
      * 删除某一列数据
      * @param tableName 表名
      * @param rowKey 行键
      * @param familyName  列族
      * @param columnName  列名
     * @throws Exception 
      */
     public static void deleteColumn(String tableName,String rowKey,
                         String familyName,String columnName) throws Exception{
         HTable hTable=new HTable(configuration, tableName);
         Delete delete=new Delete(Bytes.toBytes(rowKey));
         delete.deleteColumn(Bytes.toBytes(familyName), 
                               Bytes.toBytes(columnName));
        hTable.delete(delete);
        System.out.println(columnName+" deleted!");
     }
     /**
      * 删除某一行键下的所有列
      * @param tableName 表名
      * @param rowKey  行键
      * @throws Exception
      */
     public static void deleteAllColumn(String tableName,String rowKey) throws Exception{
         HTable hTable=new HTable(configuration, tableName);
         Delete delete=new Delete(Bytes.toBytes(rowKey));
         hTable.delete(delete);
         System.out.println("all columns deleted!");
     }
     
     /**
      * 删除整张表
      * @param tableName 表名
     * @throws Exception 
      */
     @SuppressWarnings("resource")
     public static void deleteTable(String tableName) throws Exception{
        
        HBaseAdmin hBaseAdmin=new HBaseAdmin(configuration);
         if(hBaseAdmin.tableExists(tableName)){
             hBaseAdmin.disableTable(tableName);
             hBaseAdmin.deleteTable(tableName);
             System.out.println("table deleted!");
         }
     }
}
