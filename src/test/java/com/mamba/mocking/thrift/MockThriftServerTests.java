package com.mamba.mocking.thrift;

import org.junit.jupiter.api.Test;

class MockThriftServerTests {

    @Test
    void test() throws Exception {
        MockThriftServer.main("-p", "8099", "-c", System.getProperty("user.dir") + "/sample/sample.cfg");
        System.out.println(1);
    }
}