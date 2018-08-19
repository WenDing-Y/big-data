package com.tech.demo.zookeeper;

/**
 * @author xxx_xx
 * @date 2018/8/19
 */
public class Test {
    static int n = 500;


    public static void main(String[] args) {

        Runnable runnable = new Runnable() {
            public void run() {
                TestLock lock = null;
                try {
                    lock = new TestLock("47.104.74.198:2181", "test1");
                    lock.lock();
                    System.out.println(Thread.currentThread().getName() + "正在运行");
                } finally {
                    if (lock != null) {
                        lock.unlock();
                    }
                }
            }
        };

        for (int i = 0; i < 3; i++) {
            Thread t = new Thread(runnable);
            t.start();
        }
    }
}
