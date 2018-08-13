package com.tech.demo.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * @author xxx_xx
 * @date 2018/4/26
 */
public class DistributedLock implements Lock, Watcher {

    private ZooKeeper zk;
    private String config;
    private int sessionTimeout = 30000;
    private String root = "/root";

    public DistributedLock(String config, String lockName) {
        try {
            this.zk = new ZooKeeper(config, sessionTimeout, this);
            Stat stat = zk.exists(root, false);
            if (stat == null) {
                zk.create(root, "root".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        this.config = config;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {

    }

    @Override
    public void lock() {
        if (this.tryLock()) {
            System.out.println("当前线程" + Thread.currentThread().getName() + "获得锁");
        } else {

        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {

    }

    @Override
    public boolean tryLock() {
        return false;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        //创建临时子节点
        try {
            String path = zk.create("/root/child", "child".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println("临时子节点路径" + path);
            //找到所有子节点，对子节点进行排序
            List<String> child = zk.getChildren("/root/", this);
            List<String> lockObjNodes = new ArrayList<String>();
            for (String name : child) {
                lockObjNodes.add(name.replace("child", ""));
            }
            Collections.sort(lockObjNodes);
            if (path.equals("child" + lockObjNodes.get(0))) {
                System.out.println("新建节点是最小节点" + Thread.currentThread().getName());
            }
            //找到最
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public void unlock() {

    }

    @Override
    public Condition newCondition() {
        return null;
    }
}
