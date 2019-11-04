package com.mamba.mocking.thrift.sample.client;

import com.mamba.mocking.thrift.sample.face.SharedService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.junit.jupiter.api.Test;

public class ClientTests {

    @Test
    void test_call_method1() throws Exception {
        try (TSocket socket = new TSocket("0.0.0.0", 8099, 1000 * 10)) {
            socket.open();
            long beginTime = System.currentTimeMillis();
            SharedService.Client client = new SharedService.Client(new TBinaryProtocol(socket));
            Object ret = client.getStruct(11, "xxx123", null);
            long endTime = System.currentTimeMillis();
            System.out.println("getStruct Latency: " + (endTime - beginTime));
            System.out.println("getStruct Return: " + ret);
        }
    }

    @Test
    void test_call_method2() throws Exception {
        try (TSocket socket = new TSocket("0.0.0.0", 8099, 1000 * 10)) {
            socket.open();
            long beginTime = System.currentTimeMillis();
            SharedService.Client client = new SharedService.Client(new TBinaryProtocol(socket));
            client.getStruct1(11, "xxx123", null);
            long endTime = System.currentTimeMillis();
            System.out.println("getStruct1 Latency: " + (endTime - beginTime));
        }
    }
}
