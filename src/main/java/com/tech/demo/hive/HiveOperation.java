package com.tech.demo.hive;

import java.sql.*;

/**
 * @author xxx_xx
 * @date 2018/5/1
 */
public class HiveOperation {

    private PreparedStatement statement;

    private void getDatabases() {
        Connection conn = HiveConnection.getConnection();
        String sql = "select id,name from dw.bdl_table1 limit 10";
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

    private void createTable(String sql) {
        Connection conn = HiveConnection.getConnection();
        try {
            Statement statement = conn.createStatement();
            statement.execute("use dw");
            String dropsql = sql.split(";")[1];
            String createsql = sql.split(";")[2];
            statement.execute(dropsql);
            statement.execute(createsql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        HiveOperation operation = new HiveOperation();
        // operation.getDatabases();
        String sql = "use dw;\n" +
                "drop table if exists bdl_table5;\n" +
                "CREATE EXTERNAL TABLE bdl_table5(\n" +
                "     id            STRING              COMMENT             '',\n" +
                "     name          STRING              COMMENT             '',\n" +
                "     age           STRING              COMMENT             ''\n" +
                ")\n" +
                "COMMENT ''\n" +
                "PARTITIONED BY(ymd string)\n" +
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'\n" +
                "STORED AS TEXTFILE\n" +
                "LOCATION '/hive/table/table5';";
        operation.createTable(sql);
    }
}
