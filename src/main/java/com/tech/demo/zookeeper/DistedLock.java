package com.tech.demo.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * @author xxx_xx
 * @date 2018/8/13
 */
public class DistedLock extends Thread implements Lock, Watcher {
    private ZooKeeper zk;
    private String root = "/lock";
    private String childPath = "/lock/child";
    private CountDownLatch latch = new CountDownLatch(1);

    /**
     * 初始化如果没有根结点，创建根结点
     */
    public DistedLock(String threadName) {
        super(threadName);
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
            System.out.println(Thread.currentThread().getName() + "得到锁");
        } else {
            waitLock();
        }

    }

    public void waitLock() {
        try {
            //注册监听
            zk.exists(childPath, true);
            //等待
            latch.await();
            System.out.println(Thread.currentThread().getName() + "线程重新抢锁");
            lock();
            Thread.sleep(1000);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {

    }

    @Override
    public boolean tryLock() {
        try {
            Stat stat = zk.exists(childPath, true);
            if (stat != null) {
                return false;
            } else {
                String val = zk.create(childPath, "child1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                System.out.println(Thread.currentThread().getName() + val + "节点创建");
            }
            return true;
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public void unlock() {
        try {
            zk.delete(childPath, -1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Condition newCondition() {
        return null;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent != null) {
            this.latch.countDown();
        }
    }

    public static void main(String[] args) {
        new DistedLock("线程").start();
        new DistedLock("线程1").start();
        new DistedLock("线程2").start();
        new DistedLock("线程3").start();
    }

    @Override
    public void run() {
        this.lock();
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.unlock();
    }
}
