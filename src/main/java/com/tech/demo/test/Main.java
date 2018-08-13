package com.tech.demo.test;

import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

/**
 * @author xxx_xx
 * @date 2018/5/1
 */
public class Main {

    @Before
    public void init() {
        System.out.println("init end");
    }

    @Test
    public void getValue() {
        System.out.println("world");
    }

    @Test
    public void getValuePara(String name) {
        System.out.println(name);
    }
}
