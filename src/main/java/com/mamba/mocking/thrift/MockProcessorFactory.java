package com.mamba.mocking.thrift;

import com.google.gson.Gson;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.thrift.TBaseAsyncProcessor;
import org.apache.thrift.TBaseProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MockProcessorFactory {

    private final Class<?> serviceClass;

    private final Class<?> ifaceClass;

    private final Constructor<? extends TBaseProcessor> processorConstructor;

    private final Constructor<? extends TBaseAsyncProcessor> asyncProcessorConstructor;

    private static final Gson GSON = new Gson();

    public MockProcessorFactory(Class<?> serviceClass) throws Exception {
        Map<String, Class<?>> innerClassMap = Arrays.stream(serviceClass.getClasses()).collect(Collectors.toMap(Class::getSimpleName, Function.identity()));
        Class<?> ifaceClass = innerClassMap.get("Iface");
        Class<?> asyncIface = innerClassMap.get("AsyncIface");
        Class<?> processorClass = innerClassMap.get("Processor");
        Class<?> asyncProcessorClass = innerClassMap.get("AsyncProcessor");
        if (ifaceClass == null || !ifaceClass.isInterface()
                || asyncIface == null || !asyncIface.isInterface()
                || processorClass == null || !TBaseProcessor.class.isAssignableFrom(processorClass)
                || asyncProcessorClass == null || !TBaseAsyncProcessor.class.isAssignableFrom(asyncProcessorClass)) {
            throw new IllegalStateException("Invalid service: " + serviceClass.getName());
        }
        this.serviceClass = serviceClass;
        this.ifaceClass = ifaceClass;
        this.processorConstructor = ((Class<? extends TBaseProcessor>) processorClass).getConstructor(ifaceClass);
        this.asyncProcessorConstructor = ((Class<? extends TBaseAsyncProcessor>) asyncProcessorClass).getConstructor(asyncIface);
    }

    public TBaseProcessor newProcessor(Map<String, String> mockMethodReturnMap, Map<String, Integer> mockMethodDelayMap, int defaultDelay) throws Exception {
        return newProcessor(mockMethodReturnMap, mockMethodDelayMap, defaultDelay, this.processorConstructor, (args, mockReturn) -> mockReturn.getValue());
    }

    public TBaseAsyncProcessor newAsyncProcessor(Map<String, String> mockMethodReturnMap, Map<String, Integer> mockMethodDelayMap, int defaultDelay) throws Exception {
        return newProcessor(mockMethodReturnMap, mockMethodDelayMap, defaultDelay, this.asyncProcessorConstructor, (args, mockReturn) -> {
            AsyncMethodCallback callback = (AsyncMethodCallback) args[args.length - 1];
            callback.onComplete(mockReturn.getValue());
            return null;
        });
    }

    private <T extends TProcessor> T newProcessor(Map<String, String> mockMethodReturnMap, Map<String, Integer> mockMethodDelayMap, int defaultDelay, Constructor<T> processorConstructor, BiFunction<Object[], MockReturn, Object> mockCallback) throws Exception {
        MockReturn mockReturnDefault = new MockReturn(Math.max(defaultDelay, 0), null);
        Map<String, MockReturn> mockReturnMap = genMockReturnMap(this.ifaceClass, mockMethodReturnMap, mockMethodDelayMap, mockReturnDefault.getDelay());
        Class<?> ifaceClass = processorConstructor.getParameterTypes()[0];
        MockInvocation invocation = new MockInvocation(this.serviceClass.getSimpleName(), mockReturnMap, mockReturnDefault, mockCallback);
        Object proxyIface = Proxy.newProxyInstance(ifaceClass.getClassLoader(), new Class<?>[]{ifaceClass}, invocation);
        return processorConstructor.newInstance(proxyIface);
    }

    private static Map<String, MockReturn> genMockReturnMap(Class<?> ifaceClass, Map<String, String> mockMethodReturnMap, Map<String, Integer> mockMethodDelayMap, int defaultDelay) {
        Method[] methods = ifaceClass.getMethods();
        Map<String, MockReturn> mockReturnMap = new HashMap<>((int) Math.ceil(methods.length / 0.75));
        for (Method method : methods) {
            String name = method.getName();
            String mockMethodReturn = mockMethodReturnMap.get(name);
            int mockMethodDelay = mockMethodDelayMap.getOrDefault(name, defaultDelay);
            if (mockMethodReturn == null || mockMethodReturn.trim().isEmpty()) {
                if (mockMethodDelay == defaultDelay) {
                    continue;
                }
                mockReturnMap.put(name, new MockReturn(mockMethodDelay, null));
            } else {
                mockReturnMap.put(name, new MockReturn(mockMethodDelay, GSON.fromJson(mockMethodReturn, method.getGenericReturnType())));
            }
        }
        return mockReturnMap;
    }

    @AllArgsConstructor
    private static class MockInvocation implements InvocationHandler {

        private static final Logger LOGGER = LoggerFactory.getLogger(MockInvocation.class.getSimpleName());

        private final String serviceName;

        private final Map<String, MockReturn> mockReturnMap;

        private final MockReturn mockReturnDefault;

        private final BiFunction<Object[], MockReturn, Object> mockCallback;

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String methodName = method.getName();
            MockReturn mockReturn = this.mockReturnMap.getOrDefault(methodName, this.mockReturnDefault);
            int delay = mockReturn.getDelay();
            if (delay > 0) {
                Thread.sleep(delay);
            }
            LOGGER.info("mock method: {}.{}, delay: {}", this.serviceName, methodName, delay);
            return this.mockCallback.apply(args, mockReturn);
        }
    }

    @Getter
    @AllArgsConstructor
    private static class MockReturn {

        private final int delay;

        private final Object value;
    }
}
