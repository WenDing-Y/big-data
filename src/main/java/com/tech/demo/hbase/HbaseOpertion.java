package com.tech.demo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;

/**
 * hbase 增加改查
 *
 * @author xxx_xx
 * @date 2018/4/21
 */
public class HbaseOpertion {

    HTable table = null;

    public HbaseOpertion(String tableName) {
        Configuration configuration = HBaseConfiguration.create();
        try {
            table = new HTable(configuration, tableName);
        } catch (IOException e) {
            System.out.println("得到表失败");
            e.printStackTrace();
        }
    }

    public void getData() throws Exception {
        Get get = new Get(Bytes.toBytes("1001"));
        get.addColumn(
                Bytes.toBytes("info"),
                Bytes.toBytes("name"));
        get.addColumn(
                Bytes.toBytes("info"),
                Bytes.toBytes("age"));
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println(
                    Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
                            + Bytes.toString(CellUtil.cloneQualifier(cell)) + " ->"
                            + Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    public void putData() throws Exception {
        Put put = new Put(Bytes.toBytes("1002"));
        put.addColumn(
                Bytes.toBytes("info"),
                Bytes.toBytes("name"),
                Bytes.toBytes("zhaoliu")
        );
        put.addColumn(
                Bytes.toBytes("info"),
                Bytes.toBytes("age"),
                Bytes.toBytes(25)
        );
        table.put(put);
    }

    public void delete() throws Exception {
        Delete delete = new Delete(Bytes.toBytes("10004"));
        table.delete(delete);
    }

    public void query() throws Exception {
        HTable table = null;
        ResultScanner resultScanner = null;
        try {
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes("10001"));
            scan.setStopRow(Bytes.toBytes("10003"));
            resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                System.out.println(Bytes.toString(result.getRow()));
                for (Cell cell : result.rawCells()) {
                    System.out.println(
                            Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
                                    + Bytes.toString(CellUtil.cloneQualifier(cell)) + " ->"
                                    + Bytes.toString(CellUtil.cloneValue(cell)));
                }
                System.out.println("---------------------------------------");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(resultScanner);
            IOUtils.closeStream(table);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        table.close();
        System.out.println("关闭表连接");
    }

    public static void main(String[] args) {
        HbaseOpertion opertion = new HbaseOpertion("user");
        try {
            opertion.getData();
            // opertion.putData();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}