package me.qinben.hbase;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Scanner;

/**
 * HBase工具类，实现了创建表、删除表、添加数据、删除数据、单rowKey查询、全表查询功能
 */
public class HBaseUtil {
    /**
     * 创建表
     * @param tableName 表名，格式为namespace:tableName
     * @param columnFamilies 列族名，格式为{columnFamily1, columnFamily2, ...}
     * @return 返回布尔量，true为创建成功
     */
    public static boolean createTable(String tableName, String[] columnFamilies) {
        // 拆分出命名空间
        String namespace = tableName.split(":")[0];
        // 获取admin(放在try后的括号里不必手动关闭)
        try (HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()) {
            // 获取所有命名空间
            NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
            // 命名空间是否已存在标志位
            boolean exist = false;
            // 遍历所有命名空间判断是否存在
            for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
                if (namespaceDescriptor.getName().equals(namespace)) {
                    exist = true;
                    break;
                }
            }
            // 不存在则创建命名空间
            if (!exist) {
                NamespaceDescriptor newNamespaceDescriptor = NamespaceDescriptor.create(namespace).build();
                admin.createNamespace(newNamespaceDescriptor);
            }
            // 将字符串转换为表名对象
            TableName table = TableName.valueOf(tableName);
            // 判断表是否已经存在，如果存在返回false
            if (admin.tableExists(table)) {
                return false;
            }
            // 不存在则创建表
            // 创建表描述符构建器
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(table);
            // 根据字符串创建列族描述符
            for (String columnFamily : columnFamilies) {
                tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamily));
            }
            // 创建表
            admin.createTable(tableDescriptorBuilder.build());
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 删除表
     * @param tableName 表名，格式为namespace:tableName
     * @return 返回布尔量，true代表删除成功
     */
    public static boolean deleteTable(String tableName) {
        // 将字符串转换为TableName对象
        TableName table = TableName.valueOf(tableName);
        // 获取admin
        try (HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()) {
            // 判断表是否存在，如果不存在则直接返回false
            if (!admin.tableExists(table)) {
                return false;
            }
            // 停止表并删除
            admin.disableTable(table);
            admin.deleteTable(table);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 添加数据
     * @param tableName 表名，格式为namespace:tableName
     * @param rowKey 行键
     * @param fields 列名，格式为{columnFamily1:column1, ...}
     * @param values 值
     * @return 返回布尔量，true为添加成功
     */
    public static boolean addRecord(String tableName, String rowKey, String[] fields, String[] values) {
        // 根据tableName获取表
        try (Table table = HBaseConn.getHBaseConn().getTable(TableName.valueOf(tableName))) {
            // 根据行键创建Put对象
            Put put = new Put(Bytes.toBytes(rowKey));
            // 添加数据
            for (int i = 0; i < fields.length; i++) {
                // 拆分field为列族和列标识
                String[] splits = fields[i].split(":");
                // 向put添加对应列族、列标识、值
                put.addColumn(Bytes.toBytes(splits[0]), Bytes.toBytes(splits[1]), Bytes.toBytes(values[i]));
            }
            // 向表里写入数据
            table.put(put);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 删除数据
     * @param tableName 表名，格式为namespace:tableName
     * @param rowKey 行键
     * @return 返回布尔量，true表示删除成功
     */
    public static boolean deleteRecord(String tableName, String rowKey) {
        try (Table table = HBaseConn.getHBaseConn().getTable(TableName.valueOf(tableName))) {
            // 根据rowKey创建Delete对象
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            // 从表中删除数据
            table.delete(delete);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 单rowKey查询
     * @param tableName 表名，格式为namespace:tableName
     * @param rowKey 行键
     * @return 返回Result对象，查询失败返回null
     */
    public static Result readRecord(String tableName, String rowKey) {
        try (Table table = HBaseConn.getHBaseConn().getTable(TableName.valueOf(tableName))) {
            Get get = new Get(Bytes.toBytes(rowKey));
            return table.get(get);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 全表查询
     * @param tableName 表名，格式为namespace:tableName
     * @return 返回ResultScanner对象，查询失败返回null
     */
    public static ResultScanner getScanner(String tableName) {
        try (Table table = HBaseConn.getHBaseConn().getTable(TableName.valueOf(tableName))) {
            Scan scan = new Scan();
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
