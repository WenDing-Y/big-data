package com.tech.demo.zookeeper;

/**
 * @author xxx_xx
 * @date 2018/8/13
 */

public enum ZkEvent {


    NodeCreated(1),
    NodeDeleted(2),
    NodeDataChanged(3),
    NodeChildrenChanged(4);

    private final int intValue;

    private ZkEvent(int intValue) {
        this.intValue = intValue;
    }

    public int getIntValue() {
        return this.intValue;
    }

    public static ZkEvent fromInt(int intValue) {
        switch (intValue) {
            default:
                throw new RuntimeException("Invalid integer value for conversion to EventType");
            case 1:
                return NodeCreated;
            case 2:
                return NodeDeleted;
            case 3:
                return NodeDataChanged;
            case 4:
                return NodeChildrenChanged;
        }
    }

    public static void main(String[] args) {
        System.out.println(ZkEvent.fromInt(2));
        User user = new User("world", ZkEvent.fromInt(2));
    }

}
