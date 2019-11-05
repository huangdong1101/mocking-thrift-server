package com.mamba.mocking.thrift.conf;

import com.mamba.mocking.thrift.MockProcessorFactory;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TProcessorParser {

    public static TProcessor parseProcessor(Properties props, ClassLoader classLoader) throws Exception {
        //解析Properties
        Map<String, String> classMap = new HashMap<>();
        Map<String, Integer> delayMap = new HashMap<>();
        Map<String, Map<String, Integer>> methodDelayMap = new HashMap<>();
        Map<String, Map<String, String>> methodReturnMap = new HashMap<>();
        for (String propertyName : props.stringPropertyNames()) {
            if (!propertyName.startsWith("service.")) {
                continue;
            }
            String propertyValue = props.getProperty(propertyName);
            if (propertyValue == null || propertyValue.isEmpty()) {
                continue;
            }
            String propertyValueTrim = propertyValue.trim();
            if (propertyValueTrim.isEmpty()) {
                continue;
            }
            String[] propertyNameSplits = propertyName.split("\\.");
            switch (propertyNameSplits.length) {
                case 2: //service.class=com.mamba.benchmark.thrift.sample.face.SharedService
                    classifyProperty(propertyValueTrim, "", propertyNameSplits[1], classMap, delayMap);
                    break;
                case 3: //service.name1.class=com.mamba.benchmark.thrift.sample.face.SharedService
                    classifyProperty(propertyValueTrim, propertyNameSplits[1], propertyNameSplits[2], classMap, delayMap);
                    break;
                case 4:  //service.method.xxx.delay
                    classifyProperty(propertyValueTrim, propertyNameSplits, "", methodDelayMap, methodReturnMap);
                    break;
                case 5:  //service.name1.method.xxx.delay
                    classifyProperty(propertyValueTrim, propertyNameSplits, propertyNameSplits[1], methodDelayMap, methodReturnMap);
                    break;
                default:
                    break;
            }
        }

        //创建Processor
        Map<String, TProcessor> mockProcessorMap = new HashMap<>();
        for (Map.Entry<String, String> classEntry : classMap.entrySet()) {
            String serviceType = classEntry.getValue();
            if (serviceType == null || serviceType.isEmpty()) {
                continue;
            }
            Class<?> serviceClass = classLoader.loadClass(serviceType);
            String serviceName = classEntry.getKey();
            Integer defaultDelay = delayMap.getOrDefault(serviceName, 0);
            Map<String, String> mockMethodReturnMap = methodReturnMap.getOrDefault(serviceName, Collections.emptyMap());
            Map<String, Integer> mockMethodDelayMap = methodDelayMap.getOrDefault(serviceName, Collections.emptyMap());
            MockProcessorFactory processorFactory = new MockProcessorFactory(serviceClass);
            TProcessor mockProcessor = processorFactory.newProcessor(mockMethodReturnMap, mockMethodDelayMap, defaultDelay);
            mockProcessorMap.put(serviceName, mockProcessor);
        }

        //组合Processor
        if (mockProcessorMap.isEmpty()) {
            throw new IllegalStateException("None service register in properties");
        }
        if (mockProcessorMap.size() == 1) {
            Map.Entry<String, TProcessor> processorEntry = mockProcessorMap.entrySet().iterator().next();
            String serviceName = processorEntry.getKey();
            TProcessor mockProcessor = processorEntry.getValue();
            if (serviceName.isEmpty()) {
                return mockProcessor;
            }
            TMultiplexedProcessor multiplexedProcessor = new TMultiplexedProcessor();
            multiplexedProcessor.registerProcessor(serviceName, mockProcessor);
            return multiplexedProcessor;
        }
        TMultiplexedProcessor multiplexedProcessor = new TMultiplexedProcessor();
        for (Map.Entry<String, TProcessor> processorEntry : mockProcessorMap.entrySet()) {
            String serviceName = processorEntry.getKey();
            TProcessor mockProcessor = processorEntry.getValue();
            if (serviceName.isEmpty()) {
                multiplexedProcessor.registerDefault(mockProcessor);
            } else {
                multiplexedProcessor.registerProcessor(serviceName, mockProcessor);
            }
        }
        return multiplexedProcessor;
    }

    private static void classifyProperty(String propertyValue, String serviceName, String servicePropertyName, Map<String, String> classMap, Map<String, Integer> delayMap) {
        if (servicePropertyName.equals("class")) {
            classMap.put(serviceName, propertyValue);
        } else if (servicePropertyName.equals("delay")) {
            delayMap.put(serviceName, Integer.parseInt(propertyValue));
        } else {
            //TODO
        }
    }

    private static void classifyProperty(String propertyValue, String[] propertyNameSplits, String serviceName, Map<String, Map<String, Integer>> methodsDelayMap, Map<String, Map<String, String>> methodsRetrunMap) {
        int length = propertyNameSplits.length;
        if (!propertyNameSplits[length - 3].equals("method")) {
            //TODO
            return;
        }
        String methodName = propertyNameSplits[length - 2];
        String methodPropertyName = propertyNameSplits[length - 1];
        if (methodPropertyName.equals("delay")) {
            methodsDelayMap.computeIfAbsent(serviceName, key -> new HashMap<>()).put(methodName, Integer.parseInt(propertyValue));
        } else if (methodPropertyName.equals("retrun")) {
            methodsRetrunMap.computeIfAbsent(serviceName, key -> new HashMap<>()).put(methodName, propertyValue);
        } else {
            //TODO
        }
    }
}
