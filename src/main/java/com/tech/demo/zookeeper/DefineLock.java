package com.tech.demo.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import scala.tools.cmd.gen.AnyVals;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * @author xxx_xx
 * @date 2018/8/19
 */
public class DefineLock implements Lock, Watcher {

    private ZooKeeper zk;
    private String root = "/locks";
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    private String lockName;
    private String currentName;

    public DefineLock(String lockName) {
        this.lockName = lockName;
        try {
            this.zk = new ZooKeeper("47.104.74.198:2181", 2000, this);
            Stat stat = zk.exists(root, true);
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
    }

    @Override
    public void lock() {
        if (tryLock()) {
            System.out.println(Thread.currentThread().getName() + "拿到锁了");
        } else {
            waitLock(10, TimeUnit.SECONDS);
        }
    }

    public void waitLock(long timeout, TimeUnit unit) {

    }

    @Override
    public void lockInterruptibly() throws InterruptedException {

    }

    @Override
    public boolean tryLock() {
        String splitStr = "_lock_";
        try {
            currentName = zk.create(root + "/" + lockName + splitStr, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL);
            List<String> subNodes = zk.getChildren(root, false);
            for (String node : subNodes) {

            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return true;

    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public void unlock() {

    }

    @Override
    public Condition newCondition() {
        return null;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent != null) {
            countDownLatch.countDown();
        }
    }
}
