package com.tech.demo.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.junit.Test;

import java.util.List;

/**
 * @author xxx_xx
 * @date 2018/5/1
 */
public class TestMetaStore {

    IMetaStoreClient client;


    public TestMetaStore() {
        try {
            HiveConf hiveConf = new HiveConf();
            hiveConf.addResource("hive-site.xml");
            client = RetryingMetaStoreClient.getProxy(hiveConf, false);
        } catch (MetaException ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void getAllDatabases() {
        List<String> databases = null;
        try {
            databases = client.getAllDatabases();
            for (String name : databases) {
                System.out.println("database :" + name);
            }
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    public Database getDatabase(String db) {
        Database database = null;
        try {
            database = client.getDatabase(db);
        } catch (TException ex) {
            ex.printStackTrace();
        }
        return database;
    }


    public List<FieldSchema> getSchema(String db, String table) {
        List<FieldSchema> schema = null;
        try {
            schema = client.getSchema(db, table);
        } catch (TException ex) {
            ex.printStackTrace();
        }
        return schema;
    }

    public List<String> getAllTables(String db) {
        List<String> tables = null;
        try {
            tables = client.getAllTables(db);
        } catch (TException ex) {
            ex.printStackTrace();
        }
        return tables;
    }

    public String getLocation(String db, String table) {
        String location = null;
        try {
            location = client.getTable(db, table).getSd().getLocation();
        } catch (TException ex) {
            ex.printStackTrace();
        }
        return location;
    }

    public static void main(String[] args) {
        TestMetaStore metaStore = new TestMetaStore();
        metaStore.getAllDatabases();
    }

}
