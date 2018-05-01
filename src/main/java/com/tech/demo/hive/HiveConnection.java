package com.tech.demo.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author xxx_xx
 * @date 2018/5/1
 */
public class HiveConnection {

    // private static String url = "jdbc:hive2://centos-1:2181,centos-2:2181,centos-3:2181/dw;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;connection.timeout=10000\n";
    private static String url = "jdbc:hive2://centos-1:10000/dw";
    private static String driverClassName = "org.apache.hive.jdbc.HiveDriver";

    public static Connection getConnection() {
        try {
            Class.forName(driverClassName);
            return DriverManager.getConnection(url, "", "");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        HiveConnection.getConnection();
    }
}
