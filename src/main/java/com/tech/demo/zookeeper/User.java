package com.tech.demo.zookeeper;

/**
 * @author xxx_xx
 * @date 2018/8/13
 */
public class User {

    private String name;
    private Enum<ZkEvent> ZK;

    public User(String name, Enum<ZkEvent> ZK) {
        this.name = name;
        this.ZK = ZK;
    }

    public String getName() {

        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Enum<ZkEvent> getZK() {
        return ZK;
    }

    public void setZK(Enum<ZkEvent> ZK) {
        this.ZK = ZK;
    }
}
