package com.mamba.mocking.thrift.conf;

import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

import java.util.Map;

public class TTransportFactoryParser {

    public static TTransportFactory parseTransportFactory(String define) {
        return ParameterizedPropertyParser.parse(define, TTransportFactoryParser::newTransportFactory);
    }

    public static TTransportFactory newTransportFactory(String name, Map<String, String> attrs) {
        if (TTransport.class.getSimpleName().equals(name)) {
            return new TTransportFactory();
        }
        if (TFramedTransport.class.getSimpleName().equals(name)) {
            if (attrs == null || attrs.isEmpty()) {
                return new TFramedTransport.Factory();
            }
            String maxLength = attrs.get("maxLength");
            if (maxLength == null) {
                return new TFramedTransport.Factory();
            }
            return new TFramedTransport.Factory(Integer.parseInt(maxLength));
        }
        if (TFastFramedTransport.class.getSimpleName().equals(name)) {
            if (attrs == null || attrs.isEmpty()) {
                return new TFastFramedTransport.Factory();
            }
            String initialCapacity = attrs.get("initialCapacity");
            String maxLength = attrs.get("maxLength");
            if (initialCapacity == null) {
                return new TFastFramedTransport.Factory();
            }
            if (maxLength == null) {
                return new TFastFramedTransport.Factory(Integer.parseInt(initialCapacity));
            }
            return new TFastFramedTransport.Factory(Integer.parseInt(initialCapacity), Integer.parseInt(maxLength));
        }
        throw new IllegalArgumentException("Unsupported transport: " + name);
    }
}
