package com.tech.demo.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * @author xxx_xx
 * @date 2018/4/26
 */
public class TestZookeeper implements Watcher {

    private ZooKeeper zk;

    public TestZookeeper() {
        try {
            this.zk = new ZooKeeper("47.104.74.198:2181", 2000, this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    //创建节点
    public void create() {
        try {
            Stat stat = zk.exists("/test", false);
            if (stat == null) {
                zk.create("/test", "root".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //创建临时序列子节点
    public void createEphSenChildredNode() {
        try {
            String path = zk.create("/test/child", "firstchild".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println("创建子节点路径" + path);
            //暂时休眠10s，去命令行查看临时节点
            Thread.sleep(10000);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //创建临时子节点
    public void createEphChildRedNode() {
        try {
            String path = zk.create("/test/child", "firstchild".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println("创建子节点路径" + path);
            //暂时休眠10s，去命令行查看临时节点
            Thread.sleep(10000);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent.getType());
    }

    public static void main(String[] args) {
        TestZookeeper zo = new TestZookeeper();
        zo.create();
        zo.createEphSenChildredNode();
        zo.createEphChildRedNode();
    }
}
