package com.hadoop.hbaseapi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseApi {

    public static void main(String[] args) throws IOException {
        System.setProperty("hadoop.home.dir", "/");
        //第一步：通过Zookeeper的地址和端口来获取当前活跃的master和regionServer的地址。
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "127.0.0.1");//zookeeper的地址
        configuration.set("hbase.zookeeper.property.clientPort", "2181");//端口
        configuration.set("hbase.master", "127.0.0.1:60000");
        try {
            //第二步：建立连接。
            Connection conn = ConnectionFactory.createConnection(configuration);
            //第三步：获取管理hbase数据库的表信息。
            Admin admin = conn.getAdmin();
            //第四步：判断命名空间是否存在，没有则创建命名空间。
            try {
                admin.getNamespaceDescriptor("qianlingling");
            } catch (NamespaceNotFoundException e) {
                admin.createNamespace(NamespaceDescriptor.create("qianlingling").build());
            }
            //第五步：定义表名及列族。
            TableName tableName = TableName.valueOf("qianlingling:student");
            String colFamily1 = "info";
            String colFamily2 = "score";
            //第六步：建立表。
            if (admin.tableExists(tableName)) {
                System.out.println("Table already exists");
                admin.disableTable(tableName);//禁用表
                admin.deleteTable(tableName);//删除表
                System.out.println("Table Delete Successful");
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                HColumnDescriptor hColumnDescriptor1 = new HColumnDescriptor(colFamily1);
                hTableDescriptor.addFamily(hColumnDescriptor1);
                HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor(colFamily2);
                hTableDescriptor.addFamily(hColumnDescriptor2);
                admin.createTable(hTableDescriptor);
                System.out.println("Table create successful");
            } else {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                HColumnDescriptor hColumnDescriptor1 = new HColumnDescriptor(colFamily1);
                hTableDescriptor.addFamily(hColumnDescriptor1);
                HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor(colFamily2);
                hTableDescriptor.addFamily(hColumnDescriptor2);
                admin.createTable(hTableDescriptor);
                System.out.println("Table create successful");
            }
            //第七步：定义表中添加字段的信息。
            String[] name = {"Tom", "Jerry", "Jack", "Rose", "qianlingling"};
            String[] student_id = {"20210000000001", "20210000000002", "20210000000003", "20210000000004", "G20220735020153"};
            String[] class_ = {"1", "1", "2", "2", "3"};
            String[] understanding = {"75", "85", "80", "60", "80"};
            String[] programming = {"82", "67", "80", "61", "80"};
            //第八步：循环插入数据，根据键值。
            for (int i = 0; i <= 4; i++) {
                // hbase中通常以byte输入，因此常把字符串转成byte。
                Put put = new Put(Bytes.toBytes(name[i])); // row key
                put.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("student_id"), Bytes.toBytes(student_id[i])); // col1
                put.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("class"), Bytes.toBytes(class_[i])); // col2
                put.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("understanding"), Bytes.toBytes(understanding[i])); // col1
                put.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("programming"), Bytes.toBytes(programming[i])); // col2
                conn.getTable(tableName).put(put);
                System.out.println("Data insert【" + i + "】success");
            }
            //第九步：查看数据，根据键值。
            Get get = new Get(Bytes.toBytes(name[4]));
            if (!get.isCheckExistenceOnly()) {
                Result result = conn.getTable(tableName).get(get);
                for (Cell cell : result.rawCells()) {
                    String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    System.out.println("Data get success, colName: " + colName + ", value: " + value);
                }
            }
            //第十步：删除数据
            Delete delete = new Delete(Bytes.toBytes(name[3]));      // 指定rowKey
            conn.getTable(tableName).delete(delete);
            System.out.println("Delete Success");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

