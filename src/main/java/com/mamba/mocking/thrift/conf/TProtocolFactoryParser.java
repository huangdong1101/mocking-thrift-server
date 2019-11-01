package com.mamba.mocking.thrift.conf;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.apache.thrift.protocol.TTupleProtocol;

import java.util.Map;

public class TProtocolFactoryParser {

    public static TProtocolFactory parseProtocolFactory(String define) {
        return ParameterizedPropertyParser.parse(define, TProtocolFactoryParser::newProtocolFactory);
    }

    public static TProtocolFactory newProtocolFactory(String name, Map<String, String> attrs) {
        if (TBinaryProtocol.class.getSimpleName().equals(name)) {
            if (attrs == null || attrs.isEmpty()) {
                return new TBinaryProtocol.Factory();
            }
            String strictRead = attrs.get("maxLength");
            String strictWrite = attrs.get("strictWrite");
            String stringLengthLimit = attrs.get("stringLengthLimit");
            String containerLengthLimit = attrs.get("containerLengthLimit");
            if (strictRead == null || strictWrite == null) {
                if (stringLengthLimit == null || containerLengthLimit == null) {
                    return new TBinaryProtocol.Factory();
                }
                return new TBinaryProtocol.Factory(Long.parseLong(stringLengthLimit), Long.parseLong(containerLengthLimit));
            }
            if (stringLengthLimit == null || containerLengthLimit == null) {
                return new TBinaryProtocol.Factory(Boolean.parseBoolean(strictRead), Boolean.parseBoolean(strictWrite));
            }
            return new TBinaryProtocol.Factory(Boolean.parseBoolean(strictRead), Boolean.parseBoolean(strictWrite), Long.parseLong(stringLengthLimit), Long.parseLong(containerLengthLimit));
        }
        if (TCompactProtocol.class.getSimpleName().equals(name)) {
            if (attrs == null || attrs.isEmpty()) {
                return new TCompactProtocol.Factory();
            }
            String stringLengthLimit = attrs.get("stringLengthLimit");
            String containerLengthLimit = attrs.get("containerLengthLimit");
            if (stringLengthLimit == null) {
                return new TCompactProtocol.Factory();
            }
            if (containerLengthLimit == null) {
                return new TCompactProtocol.Factory(Long.parseLong(stringLengthLimit));
            }
            return new TCompactProtocol.Factory(Long.parseLong(stringLengthLimit), Long.parseLong(containerLengthLimit));
        }
        if (TTupleProtocol.class.getSimpleName().equals(name)) {
            return new TTupleProtocol.Factory();
        }
        if (TJSONProtocol.class.getSimpleName().equals(name)) {
            if (attrs == null || attrs.isEmpty()) {
                return new TJSONProtocol.Factory();
            }
            String fieldNamesAsString = attrs.get("fieldNamesAsString");
            if (fieldNamesAsString == null) {
                return new TJSONProtocol.Factory();
            }
            return new TJSONProtocol.Factory(Boolean.parseBoolean(fieldNamesAsString));
        }
        if (TSimpleJSONProtocol.class.getSimpleName().equals(name)) {
            return new TSimpleJSONProtocol.Factory();
        }
        throw new IllegalArgumentException("Unsupported protocol: " + name);
    }
}
