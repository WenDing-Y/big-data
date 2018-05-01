package com.tech.demo.hive;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author xxx_xx
 * @date 2018/5/1
 */
public class HiveOperation {

    private PreparedStatement statement;

    private void getDatabases() {
        Connection conn = HiveConnection.getConnection();
        String sql = "select * from dw.bdl_table1";
        try {
            statement = conn.prepareStatement(sql);
            ResultSet set = statement.executeQuery();
            while (set.next()) {
                System.out.println(set.getString("id"));
                System.out.println(set.getString("name"));

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        HiveOperation operation = new HiveOperation();
        operation.getDatabases();
    }
}
