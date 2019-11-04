package com.mamba.mocking.thrift.conf;

import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class TServerManager<T extends TServerTransport, S extends TServer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TServerManager.class);

    private TServerTransportFactory<T> serverTransportFactory;

    private TServerFactory<T, S> serverFactory;

    private Map<String, String> attrs;

    public TServerManager(TServerTransportFactory<T> serverTransportFactory, TServerFactory<T, S> serverFactory, Map<String, String> attrs) {
        this.serverTransportFactory = serverTransportFactory;
        this.serverFactory = serverFactory;
        this.attrs = attrs == null ? Collections.emptyMap() : attrs;
    }

    public void serve(int port, TProcessor processor, TTransportFactory transportFactory, TProtocolFactory protocolFactory) throws TTransportException {
        try (T transport = this.serverTransportFactory.newTransport(port)) {
            LOGGER.info("=========Thrift server starting=======");
            LOGGER.info("Listen port: {}", port);
            S server = this.serverFactory.newServer(transport, processor, transportFactory, protocolFactory, this.attrs);
            LOGGER.info("=========Thrift server started=======");
            server.serve();
            LOGGER.error("Thrift server stopped...");
            server.stop();
        }
    }

    public static TServerManager<?, ?> newInstance(String description) {
        if (description == null || description.isEmpty()) {
            return new TServerManager<>(TServerSocket::new, TServerManager::newTSimpleServer, Collections.emptyMap());
        } else {
            return ParameterizedPropertyParser.parse(description, TServerManager::newInstance);
        }
    }

    public static TServerManager<?, ?> newInstance(String name, Map<String, String> attrs) {
        if (TSimpleServer.class.getSimpleName().equals(name)) {
            return new TServerManager<>(TServerSocket::new, TServerManager::newTSimpleServer, attrs);
        } else if (TThreadPoolServer.class.getSimpleName().equals(name)) {
            return new TServerManager<>(TServerSocket::new, TServerManager::newTThreadPoolServer, attrs);
        } else if (TNonblockingServer.class.getSimpleName().equals(name)) {
            return new TServerManager<>(TNonblockingServerSocket::new, TServerManager::newTNonblockingServer, attrs);
        } else if (THsHaServer.class.getSimpleName().equals(name)) {
            return new TServerManager<>(TNonblockingServerSocket::new, TServerManager::newTHsHaServer, attrs);
        } else if (TThreadedSelectorServer.class.getSimpleName().equals(name)) {
            return new TServerManager<>(TNonblockingServerSocket::new, TServerManager::newTThreadedSelectorServer, attrs);
        } else {
            throw new IllegalArgumentException("Unsupported server: " + name);
        }
    }

    private static TSimpleServer newTSimpleServer(TServerTransport transport, TProcessor processor, TTransportFactory transportFactory, TProtocolFactory protocolFactory, Map<String, String> attrs) {
        TSimpleServer.Args args = new TSimpleServer.Args(transport);
        args.processorFactory(new TProcessorFactory(processor));
        setValue(args, transportFactory, TSimpleServer.Args::transportFactory);
        setValue(args, protocolFactory, TSimpleServer.Args::protocolFactory);
        return new TSimpleServer(args);
    }

    private static TThreadPoolServer newTThreadPoolServer(TServerTransport transport, TProcessor processor, TTransportFactory transportFactory, TProtocolFactory protocolFactory, Map<String, String> attrs) {
        TThreadPoolServer.Args args = new TThreadPoolServer.Args(transport);
        args.processorFactory(new TProcessorFactory(processor));
        setValue(args, transportFactory, TThreadPoolServer.Args::transportFactory);
        setValue(args, protocolFactory, TThreadPoolServer.Args::protocolFactory);
        if (attrs != null && !attrs.isEmpty()) {
            setValue(args, attrs.get("minWorkerThreads"), (target, minWorkerThreads) -> target.minWorkerThreads(Integer.parseInt(minWorkerThreads)));
            setValue(args, attrs.get("maxWorkerThreads"), (target, maxWorkerThreads) -> target.maxWorkerThreads(Integer.parseInt(maxWorkerThreads)));
            setValue(args, attrs.get("stopTimeoutVal"), (target, stopTimeoutVal) -> target.stopTimeoutVal(Integer.parseInt(stopTimeoutVal)));
            setValue(args, attrs.get("stopTimeoutUnit"), (target, stopTimeoutUnit) -> target.stopTimeoutUnit(TimeUnit.valueOf(stopTimeoutUnit)));
            setValue(args, attrs.get("requestTimeout"), (target, requestTimeout) -> target.requestTimeout(Integer.parseInt(requestTimeout)));
            setValue(args, attrs.get("requestTimeoutUnit"), (target, requestTimeoutUnit) -> target.requestTimeoutUnit(TimeUnit.valueOf(requestTimeoutUnit)));
            setValue(args, attrs.get("beBackoffSlotLength"), (target, beBackoffSlotLength) -> target.beBackoffSlotLength(Integer.parseInt(beBackoffSlotLength)));
            setValue(args, attrs.get("beBackoffSlotLengthUnit"), (target, beBackoffSlotLengthUnit) -> target.beBackoffSlotLengthUnit(TimeUnit.valueOf(beBackoffSlotLengthUnit)));
        }
//        args.executorService(Executors.newCachedThreadPool());
        return new TThreadPoolServer(args);
    }

    private static TNonblockingServer newTNonblockingServer(TNonblockingServerTransport transport, TProcessor processor, TTransportFactory transportFactory, TProtocolFactory protocolFactory, Map<String, String> attrs) {
        TNonblockingServer.Args args = new TNonblockingServer.Args(transport);
        args.processorFactory(new TProcessorFactory(processor));
        setValue(args, transportFactory, TNonblockingServer.Args::transportFactory);
        setValue(args, protocolFactory, TNonblockingServer.Args::protocolFactory);
        if (attrs != null && !attrs.isEmpty()) {
            setValue(args, attrs.get("maxReadBufferBytes"), (target, maxReadBufferBytes) -> target.maxReadBufferBytes = Long.parseLong(maxReadBufferBytes));
        }
        return new TNonblockingServer(args);
    }

    private static THsHaServer newTHsHaServer(TNonblockingServerTransport transport, TProcessor processor, TTransportFactory transportFactory, TProtocolFactory protocolFactory, Map<String, String> attrs) {
        THsHaServer.Args args = new THsHaServer.Args(transport);
        args.processorFactory(new TProcessorFactory(processor));
        setValue(args, transportFactory, THsHaServer.Args::transportFactory);
        setValue(args, protocolFactory, THsHaServer.Args::protocolFactory);
        if (attrs != null && !attrs.isEmpty()) {
            setValue(args, attrs.get("maxReadBufferBytes"), (target, maxReadBufferBytes) -> target.maxReadBufferBytes = Long.parseLong(maxReadBufferBytes));
            setValue(args, attrs.get("minWorkerThreads"), (target, minWorkerThreads) -> target.minWorkerThreads(Integer.parseInt(minWorkerThreads)));
            setValue(args, attrs.get("maxWorkerThreads"), (target, maxWorkerThreads) -> target.maxWorkerThreads(Integer.parseInt(maxWorkerThreads)));
            setValue(args, attrs.get("stopTimeoutVal"), (target, stopTimeoutVal) -> target.stopTimeoutVal(Integer.parseInt(stopTimeoutVal)));
            setValue(args, attrs.get("stopTimeoutUnit"), (target, stopTimeoutUnit) -> target.stopTimeoutUnit(TimeUnit.valueOf(stopTimeoutUnit)));
        }
//        args.executorService(Executors.newCachedThreadPool());
        return new THsHaServer(args);
    }

    private static TThreadedSelectorServer newTThreadedSelectorServer(TNonblockingServerTransport transport, TProcessor processor, TTransportFactory transportFactory, TProtocolFactory protocolFactory, Map<String, String> attrs) {
        TThreadedSelectorServer.Args args = new TThreadedSelectorServer.Args(transport);
        args.processorFactory(new TProcessorFactory(processor));
        setValue(args, transportFactory, TThreadedSelectorServer.Args::transportFactory);
        setValue(args, protocolFactory, TThreadedSelectorServer.Args::protocolFactory);
        if (attrs != null && !attrs.isEmpty()) {
            setValue(args, attrs.get("maxReadBufferBytes"), (target, maxReadBufferBytes) -> target.maxReadBufferBytes = Long.parseLong(maxReadBufferBytes));
            setValue(args, attrs.get("selectorThreads"), (target, selectorThreads) -> target.selectorThreads(Integer.parseInt(selectorThreads)));
            setValue(args, attrs.get("workerThreads"), (target, workerThreads) -> target.workerThreads(Integer.parseInt(workerThreads)));
            setValue(args, attrs.get("stopTimeoutVal"), (target, stopTimeoutVal) -> target.stopTimeoutVal(Integer.parseInt(stopTimeoutVal)));
            setValue(args, attrs.get("stopTimeoutUnit"), (target, stopTimeoutUnit) -> target.stopTimeoutUnit(TimeUnit.valueOf(stopTimeoutUnit)));
            setValue(args, attrs.get("acceptQueueSizePerThread"), (target, acceptQueueSizePerThread) -> target.acceptQueueSizePerThread(Integer.parseInt(acceptQueueSizePerThread)));
            setValue(args, attrs.get("acceptPolicy"), (target, acceptPolicy) -> target.acceptPolicy(TThreadedSelectorServer.Args.AcceptPolicy.valueOf(acceptPolicy)));
        }
//        args.executorService(Executors.newCachedThreadPool());
        return new TThreadedSelectorServer(args);
    }

    private static <T, V> void setValue(T target, V value, BiConsumer<T, V> consumer) {
        if (value != null) {
            consumer.accept(target, value);
        }
    }

    private interface TServerTransportFactory<T extends TServerTransport> {

        T newTransport(int port) throws TTransportException;
    }

    private interface TServerFactory<T extends TServerTransport, S extends TServer> {

        S newServer(T transport, TProcessor processor, TTransportFactory transportFactory, TProtocolFactory protocolFactory, Map<String, String> attrs);
    }
}
