package com.mamba.mocking.thrift;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.mamba.mocking.thrift.conf.TProcessorParser;
import com.mamba.mocking.thrift.conf.TProtocolFactoryParser;
import com.mamba.mocking.thrift.conf.TServerManager;
import com.mamba.mocking.thrift.conf.TTransportFactoryParser;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;

public class MockThriftServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MockThriftServer.class);

    @Parameter(names = {"-c", "--conf"}, description = "mock conf", required = true)
    private File conf;

    @Parameter(names = {"-p", "--port"}, description = "port", required = true)
    private int port;

    public void serve() throws Exception {
        Properties props = new Properties();
        try (FileInputStream stream = new FileInputStream(this.conf)) {
            props.load(stream);
        }
        URL classpath = toURI(props.getProperty("classpath"), this.conf).toURL();
        try (URLClassLoader classLoader = URLClassLoader.newInstance(new URL[]{classpath}, Thread.currentThread().getContextClassLoader())) {
            TProcessor processor = TProcessorParser.parseProcessor(props, classLoader);
            TTransportFactory transportFactory = TTransportFactoryParser.parseTransportFactory(props.getProperty("transport"));
            TProtocolFactory protocolFactory = TProtocolFactoryParser.parseProtocolFactory(props.getProperty("protocol"));
            TServerManager serverManager = TServerManager.newInstance(props.getProperty("server"));
            serverManager.serve(this.port, processor, transportFactory, protocolFactory);
        }
    }

    private static URI toURI(String path, File conf) {
        if (path == null || path.isEmpty()) {
            throw new IllegalStateException("classpath is empty!");
        }
        File file;
        if (path.charAt(0) == '/') {
            file = new File(path);
        } else {
            file = new File(conf.getParent(), path);
        }
        if (file.exists()) {
            return file.toURI();
        } else {
            return URI.create(path);
        }
    }

    public static void main(String... args) throws Exception {
        LOGGER.info("Command Args: {}", join(args));
        MockThriftServer provider = new MockThriftServer();
        JCommander.newBuilder().addObject(provider).build().parse(args);
        provider.serve();
    }

    private static String join(String... strs) {
        if (strs == null || strs.length == 0) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (String s : strs) {
            sb.append(' ').append(s);
        }
        return sb.substring(1);
    }
}
