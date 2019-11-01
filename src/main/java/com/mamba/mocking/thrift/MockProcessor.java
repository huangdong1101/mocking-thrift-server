package com.mamba.mocking.thrift;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MockProcessor implements TProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(MockProcessor.class);

    private final TProcessor processor;

    public MockProcessor(Class<?> serviceClass, Map<String, String> mockMethodReturnMap, Map<String, Integer> mockMethodDelayMap, int defaultDelay) throws Exception {
        Map<String, Class<?>> innerClassMap = Arrays.stream(serviceClass.getClasses()).collect(Collectors.toMap(Class::getSimpleName, Function.identity()));
        Class<?> ifaceClass = innerClassMap.get("Iface");
        if (ifaceClass == null || !ifaceClass.isInterface()) {
            throw new IllegalStateException("Invalid service: " + serviceClass.getName());
        }
        Class<? extends TProcessor> processorClass = (Class<? extends TProcessor>) innerClassMap.get("Processor");
        if (processorClass == null) {
            throw new IllegalStateException("Invalid service: " + serviceClass.getName());
        }
        MockReturn mockReturnDefault = new MockReturn(Math.max(defaultDelay, 0), null);
        Map<Method, MockReturn> mockReturnMap = genMockReturnMap(ifaceClass, mockMethodReturnMap, mockMethodDelayMap, mockReturnDefault.getDelay());
        Constructor<? extends TProcessor> processorConstructor = processorClass.getConstructor(ifaceClass);
        MockInvocation invocation = new MockInvocation(mockReturnMap, mockReturnDefault);
        Object proxyIface = Proxy.newProxyInstance(ifaceClass.getClassLoader(), new Class<?>[]{ifaceClass}, invocation);
        this.processor = processorConstructor.newInstance(proxyIface);
    }

    private static Map<Method, MockReturn> genMockReturnMap(Class<?> ifaceClass, Map<String, String> mockMethodReturnMap, Map<String, Integer> mockMethodDelayMap, int defaultDelay) {
        Method[] methods = ifaceClass.getMethods();
        Map<Method, MockReturn> mockReturnMap = new HashMap<>((int) Math.ceil(methods.length / 0.75));
        for (Method method : methods) {
            String name = method.getName();
            String mockMethodReturn = mockMethodReturnMap.get(name);
            int mockMethodDelay = mockMethodDelayMap.getOrDefault(name, defaultDelay);
            if (mockMethodReturn == null || mockMethodReturn.trim().isEmpty()) {
                if (mockMethodDelay == defaultDelay) {
                    continue;
                } else {
                    mockReturnMap.put(method, new MockReturn(mockMethodDelay, null));
                }
            } else {
                mockReturnMap.put(method, new MockReturn(mockMethodDelay, JSON.parseObject(mockMethodReturn, method.getGenericReturnType())));
            }
        }
        return mockReturnMap;
    }

    @Override
    public boolean process(TProtocol in, TProtocol out) throws TException {
        return this.processor.process(in, out);
    }

    @AllArgsConstructor
    private static class MockInvocation implements InvocationHandler {

        private final Map<Method, MockReturn> mockReturnMap;

        private final MockReturn mockReturnDefault;

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            MockReturn mockReturn = this.mockReturnMap.getOrDefault(method, this.mockReturnDefault);
            int delay = mockReturn.getDelay();
            if (delay > 0) {
                Thread.sleep(delay);
            }
            LOGGER.info("mock method: {}, delay: {}", method, delay);
            return mockReturn.getValue();
        }
    }

    @Getter
    @AllArgsConstructor
    private static class MockReturn {

        private final int delay;

        private final Object value;
    }
}
