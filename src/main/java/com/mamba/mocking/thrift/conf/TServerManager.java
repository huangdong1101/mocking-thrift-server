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

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TServerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(TServerManager.class);

    public static void serve(int port, TProcessor processor, TTransportFactory transportFactory, TProtocolFactory protocolFactory, String serverType, Map<String, String> serverAttrs) throws TTransportException {
        if (TSimpleServer.class.getSimpleName().equals(serverType)) {
            serve(port, processor, transportFactory, protocolFactory, serverAttrs, TServerSocket::new, TServerManager::newTSimpleServer);
        } else if (TThreadPoolServer.class.getSimpleName().equals(serverType)) {
            serve(port, processor, transportFactory, protocolFactory, serverAttrs, TServerSocket::new, TServerManager::newTThreadPoolServer);
        } else if (TNonblockingServer.class.getSimpleName().equals(serverType)) {
            serve(port, processor, transportFactory, protocolFactory, serverAttrs, TNonblockingServerSocket::new, TServerManager::newTNonblockingServer);
        } else if (THsHaServer.class.getSimpleName().equals(serverType)) {
            serve(port, processor, transportFactory, protocolFactory, serverAttrs, TNonblockingServerSocket::new, TServerManager::newTHsHaServer);
        } else if (TThreadedSelectorServer.class.getSimpleName().equals(serverType)) {
            serve(port, processor, transportFactory, protocolFactory, serverAttrs, TNonblockingServerSocket::new, TServerManager::newTThreadedSelectorServer);
        } else {
            throw new IllegalArgumentException("Unsupported server: " + serverType);
        }
    }

    private static <T extends TServerTransport, S extends TServer> void serve(int port, TProcessor processor, TTransportFactory transportFactory, TProtocolFactory protocolFactory, Map<String, String> attrs, TServerTransportFactory<T> serverTransportFactory, TServerFactory<T, S> serverFactory) throws TTransportException {
        try (T transport = serverTransportFactory.newTransport(port)) {
            LOGGER.info("=========Thrift server starting=======");
            LOGGER.info("Listen port: {}", port);
            S server = serverFactory.newServer(transport, processor, transportFactory, protocolFactory, attrs);
            LOGGER.info("=========Thrift server started=======");
            server.serve();
            LOGGER.error("Thrift server stopped...");
            server.stop();
        }
    }

    private static TSimpleServer newTSimpleServer(TServerTransport transport, TProcessor processor, TTransportFactory transportFactory, TProtocolFactory protocolFactory, Map<String, String> attrs) {
        TSimpleServer.Args args = new TSimpleServer.Args(transport);
        args.processorFactory(new TProcessorFactory(processor));
        ParameterizedPropertyParser.setValue(args, transportFactory, TSimpleServer.Args::transportFactory);
        ParameterizedPropertyParser.setValue(args, protocolFactory, TSimpleServer.Args::protocolFactory);
        return new TSimpleServer(args);
    }

    private static TThreadPoolServer newTThreadPoolServer(TServerTransport transport, TProcessor processor, TTransportFactory transportFactory, TProtocolFactory protocolFactory, Map<String, String> attrs) {
        TThreadPoolServer.Args args = new TThreadPoolServer.Args(transport);
        args.processorFactory(new TProcessorFactory(processor));
        ParameterizedPropertyParser.setValue(args, transportFactory, TThreadPoolServer.Args::transportFactory);
        ParameterizedPropertyParser.setValue(args, protocolFactory, TThreadPoolServer.Args::protocolFactory);
        if (attrs != null && !attrs.isEmpty()) {
            ParameterizedPropertyParser.setValue(args, attrs.get("minWorkerThreads"), (target, minWorkerThreads) -> target.minWorkerThreads(Integer.parseInt(minWorkerThreads)));
            ParameterizedPropertyParser.setValue(args, attrs.get("maxWorkerThreads"), (target, maxWorkerThreads) -> target.maxWorkerThreads(Integer.parseInt(maxWorkerThreads)));
            ParameterizedPropertyParser.setValue(args, attrs.get("stopTimeoutVal"), (target, stopTimeoutVal) -> target.stopTimeoutVal(Integer.parseInt(stopTimeoutVal)));
            ParameterizedPropertyParser.setValue(args, attrs.get("stopTimeoutUnit"), (target, stopTimeoutUnit) -> target.stopTimeoutUnit(TimeUnit.valueOf(stopTimeoutUnit)));
            ParameterizedPropertyParser.setValue(args, attrs.get("requestTimeout"), (target, requestTimeout) -> target.requestTimeout(Integer.parseInt(requestTimeout)));
            ParameterizedPropertyParser.setValue(args, attrs.get("requestTimeoutUnit"), (target, requestTimeoutUnit) -> target.requestTimeoutUnit(TimeUnit.valueOf(requestTimeoutUnit)));
            ParameterizedPropertyParser.setValue(args, attrs.get("beBackoffSlotLength"), (target, beBackoffSlotLength) -> target.beBackoffSlotLength(Integer.parseInt(beBackoffSlotLength)));
            ParameterizedPropertyParser.setValue(args, attrs.get("beBackoffSlotLengthUnit"), (target, beBackoffSlotLengthUnit) -> target.beBackoffSlotLengthUnit(TimeUnit.valueOf(beBackoffSlotLengthUnit)));
        }
//        args.executorService(Executors.newCachedThreadPool());
        return new TThreadPoolServer(args);
    }

    private static TNonblockingServer newTNonblockingServer(TNonblockingServerTransport transport, TProcessor processor, TTransportFactory transportFactory, TProtocolFactory protocolFactory, Map<String, String> attrs) {
        TNonblockingServer.Args args = new TNonblockingServer.Args(transport);
        args.processorFactory(new TProcessorFactory(processor));
        ParameterizedPropertyParser.setValue(args, transportFactory, TNonblockingServer.Args::transportFactory);
        ParameterizedPropertyParser.setValue(args, protocolFactory, TNonblockingServer.Args::protocolFactory);
        if (attrs != null && !attrs.isEmpty()) {
            ParameterizedPropertyParser.setValue(args, attrs.get("maxReadBufferBytes"), (target, maxReadBufferBytes) -> target.maxReadBufferBytes = Long.parseLong(maxReadBufferBytes));
        }
        return new TNonblockingServer(args);
    }

    private static THsHaServer newTHsHaServer(TNonblockingServerTransport transport, TProcessor processor, TTransportFactory transportFactory, TProtocolFactory protocolFactory, Map<String, String> attrs) {
        THsHaServer.Args args = new THsHaServer.Args(transport);
        args.processorFactory(new TProcessorFactory(processor));
        ParameterizedPropertyParser.setValue(args, transportFactory, THsHaServer.Args::transportFactory);
        ParameterizedPropertyParser.setValue(args, protocolFactory, THsHaServer.Args::protocolFactory);
        if (attrs != null && !attrs.isEmpty()) {
            ParameterizedPropertyParser.setValue(args, attrs.get("maxReadBufferBytes"), (target, maxReadBufferBytes) -> target.maxReadBufferBytes = Long.parseLong(maxReadBufferBytes));
            ParameterizedPropertyParser.setValue(args, attrs.get("minWorkerThreads"), (target, minWorkerThreads) -> target.minWorkerThreads(Integer.parseInt(minWorkerThreads)));
            ParameterizedPropertyParser.setValue(args, attrs.get("maxWorkerThreads"), (target, maxWorkerThreads) -> target.maxWorkerThreads(Integer.parseInt(maxWorkerThreads)));
            ParameterizedPropertyParser.setValue(args, attrs.get("stopTimeoutVal"), (target, stopTimeoutVal) -> target.stopTimeoutVal(Integer.parseInt(stopTimeoutVal)));
            ParameterizedPropertyParser.setValue(args, attrs.get("stopTimeoutUnit"), (target, stopTimeoutUnit) -> target.stopTimeoutUnit(TimeUnit.valueOf(stopTimeoutUnit)));
        }
//        args.executorService(Executors.newCachedThreadPool());
        return new THsHaServer(args);
    }

    private static TThreadedSelectorServer newTThreadedSelectorServer(TNonblockingServerTransport transport, TProcessor processor, TTransportFactory transportFactory, TProtocolFactory protocolFactory, Map<String, String> attrs) {
        TThreadedSelectorServer.Args args = new TThreadedSelectorServer.Args(transport);
        args.processorFactory(new TProcessorFactory(processor));
        ParameterizedPropertyParser.setValue(args, transportFactory, TThreadedSelectorServer.Args::transportFactory);
        ParameterizedPropertyParser.setValue(args, protocolFactory, TThreadedSelectorServer.Args::protocolFactory);
        if (attrs != null && !attrs.isEmpty()) {
            ParameterizedPropertyParser.setValue(args, attrs.get("maxReadBufferBytes"), (target, maxReadBufferBytes) -> target.maxReadBufferBytes = Long.parseLong(maxReadBufferBytes));
            ParameterizedPropertyParser.setValue(args, attrs.get("selectorThreads"), (target, selectorThreads) -> target.selectorThreads(Integer.parseInt(selectorThreads)));
            ParameterizedPropertyParser.setValue(args, attrs.get("workerThreads"), (target, workerThreads) -> target.workerThreads(Integer.parseInt(workerThreads)));
            ParameterizedPropertyParser.setValue(args, attrs.get("stopTimeoutVal"), (target, stopTimeoutVal) -> target.stopTimeoutVal(Integer.parseInt(stopTimeoutVal)));
            ParameterizedPropertyParser.setValue(args, attrs.get("stopTimeoutUnit"), (target, stopTimeoutUnit) -> target.stopTimeoutUnit(TimeUnit.valueOf(stopTimeoutUnit)));
            ParameterizedPropertyParser.setValue(args, attrs.get("acceptQueueSizePerThread"), (target, acceptQueueSizePerThread) -> target.acceptQueueSizePerThread(Integer.parseInt(acceptQueueSizePerThread)));
            ParameterizedPropertyParser.setValue(args, attrs.get("acceptPolicy"), (target, acceptPolicy) -> target.acceptPolicy(TThreadedSelectorServer.Args.AcceptPolicy.valueOf(acceptPolicy)));
        }
//        args.executorService(Executors.newCachedThreadPool());
        return new TThreadedSelectorServer(args);
    }

    private interface TServerTransportFactory<T extends TServerTransport> {

        T newTransport(int port) throws TTransportException;
    }

    private interface TServerFactory<T extends TServerTransport, S extends TServer> {

        S newServer(T transport, TProcessor processor, TTransportFactory transportFactory, TProtocolFactory protocolFactory, Map<String, String> attrs);
    }
}
