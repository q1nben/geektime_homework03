package me.qinben.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * 管理HBase连接的类
 */
public class HBaseConn {
    // 创建单例对象
    private static final HBaseConn INSTANCE = new HBaseConn();
    // hbase配置
    private static Configuration conf;
    // hbase连接
    private static Connection conn;

    // 私有构造方法
    private HBaseConn() {
        // 首次连接初始化hbase配置
        if (conf == null) {
            // 创建hbase配置实例
            conf = HBaseConfiguration.create();
            // 设置zk地址(本地)
            conf.set("hbase.zookeeper.quorum", "jikehadoop01,jikehadoop02,jikehadoop03");
        }
    }

    // 获取hbase连接
    private Connection getConn() {
        // 首次连接或连接已关闭，创建连接
        if (conn == null || conn.isClosed()) {
            try {
                conn = ConnectionFactory.createConnection(conf);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return conn;
    }

    // 对外暴露获取连接方法
    public static Connection getHBaseConn() {
        return INSTANCE.getConn();
    }

    // 关闭连接
    public static void closeHBaseConn() {
        if (conn != null) {
            try {
                conn.close();
                conn = null;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
