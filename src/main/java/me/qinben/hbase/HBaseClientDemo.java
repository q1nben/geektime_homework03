package me.qinben.hbase;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class HBaseClientDemo {
    public static void main(String[] args) {
        // 获取连接
        Connection conn = HBaseConn.getHBaseConn();
        // 表名字符串
        String tableName = "qinben:student";
        // 列族字符串数组
        String[] columnFamilies = {"info", "score"};
        // 行键字符串数组
        String[] rowKeys = {"Tom", "Jerry", "Jack", "Rose", "qinben"};
        // 列标识数组
        String[] columns = {"info:student_id", "info:class", "score:understanding", "score:programming"};
        // 需要插入的数据
        String[][] values = {
                {"20210000000001", "1", "75", "82"},
                {"20210000000002", "1", "85", "67"},
                {"20210000000003", "2", "80", "80"},
                {"20210000000004", "2", "60", "61"},
                {"G20210735010190", "2", "100", "100"}
        };
        // 创建失败输出
        if (!HBaseUtil.createTable(tableName, columnFamilies)) {
            System.out.println("创建表失败");
        } else {
            // 成功创建表后插入数据
            boolean success;
            for (int i = 0; i < rowKeys.length; i++) {
                success = HBaseUtil.addRecord(tableName, rowKeys[i], columns, values[i]);
                if (!success) {
                    System.out.println("插入数据失败");
                    return;
                }
            }
            // 全表查询
            ResultScanner results = HBaseUtil.getScanner(tableName);
            if (results != null) {
                results.forEach(result -> System.out.println(result.toString()));
                results.close();
            } else {
                System.out.println("查询数据失败");
            }
        }
        try {
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
