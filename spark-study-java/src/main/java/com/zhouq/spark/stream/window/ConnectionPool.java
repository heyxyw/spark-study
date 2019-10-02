package com.zhouq.spark.stream.window;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

/**
 * Create by zhouq on 2019/10/2
 */
public class ConnectionPool {
    private static LinkedList<Connection> connectionQueue;

    /**
     * 加载驱动
     */
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取连接，多线程访问并发控制
     *
     * @return
     */
    public synchronized static Connection getConnection() {
        try {
            if (connectionQueue == null) {
                connectionQueue = new LinkedList<Connection>();
                for (int i = 0; i < 10; i++) {
                    Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/testdb", "root", "root");
                    connectionQueue.push(connection);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return connectionQueue.poll();
    }

    /**
     * 归还连接
     * @param conn
     */
    public static void returnConnectiuon(Connection conn) {
        connectionQueue.push(conn);
    }
}
