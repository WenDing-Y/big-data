package com.tech.demo.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

/**
 * @author xxx_xx
 * @date 2018/4/26
 * <p>
 * 可以注册watcher的方法：getData、exists、getChildren。
 * 可以触发watcher的方法：create、delete、setData。连接断开的情况下触发的watcher会丢失。
 * 一个Watcher实例是一个回调函数，被回调一次后就被移除了。如果还需要关注数据的变化，需要再次注册watcher。
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
            Stat stat = zk.exists("/test", true);
            if (stat == null) {
                zk.create("/test", "root".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            zk.exists("/test", true);
            Thread.sleep(10000);
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
            Thread.sleep(20000);
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
            Thread.sleep(20000);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //创建永久子节点
    public void createPreChildredNode() {
        try {
            String path = zk.create("/test/child1", "child1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("创建子节点路径" + path);
            //暂时休眠10s，去命令行查看临时节点
            Thread.sleep(20000);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //创建永久序列子节点
    public void createPreSeqChildredNode() {
        try {
            String path = zk.create("/test/child2", "child2".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            System.out.println("创建子节点路径" + path);
            //暂时休眠10s，去命令行查看临时节点
            Thread.sleep(20000);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void process(WatchedEvent watchedEvent) {
        Event.EventType type = watchedEvent.getType();
        switch (type) {
            case NodeCreated:
                System.out.println("节点创建事件" + "路径为" + watchedEvent.getPath());
            case NodeDeleted:
                System.out.println("节点删除事件" + "路径为" + watchedEvent.getPath());
            case NodeDataChanged:
                try {
                    System.out.println("节点数据改变事件" + watchedEvent.getType() + zk.getData(watchedEvent.getPath(), true, null));
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            case NodeChildrenChanged:
                try {
                    List<String> names = zk.getChildren(watchedEvent.getPath(), true);
                    System.out.println("当前子节点路径" + watchedEvent.getPath());
                    for (String nodeName : names) {
                        System.out.println("子节点name" + nodeName);
                    }
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
        }
    }

    public static void main(String[] args) {
        TestZookeeper zo = new TestZookeeper();
        zo.create();
        zo.createPreChildredNode();
        zo.createPreSeqChildredNode();
        // zo.createEphSenChildredNode();
        // zo.createEphChildRedNode();
    }
}
