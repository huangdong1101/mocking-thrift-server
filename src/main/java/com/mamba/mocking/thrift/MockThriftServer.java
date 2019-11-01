package com.mamba.mocking.thrift;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.mamba.mocking.thrift.conf.ParameterizedPropertyParser;
import com.mamba.mocking.thrift.conf.TProcessorParser;
import com.mamba.mocking.thrift.conf.TProtocolFactoryParser;
import com.mamba.mocking.thrift.conf.TServerManager;
import com.mamba.mocking.thrift.conf.TTransportFactoryParser;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransportFactory;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;

public class MockThriftServer {

    @Parameter(names = {"-c", "--conf"}, description = "mock conf", required = true)
    private File conf;

    @Parameter(names = {"-p", "--port"}, description = "port", required = true)
    private int port;

    public void serve() throws Exception {
        Properties props = new Properties();
        try (FileInputStream stream = new FileInputStream(this.conf)) {
            props.load(stream);
        }
        URL classpath = toURI(props.getProperty("classpath")).toURL();
        try (URLClassLoader classLoader = URLClassLoader.newInstance(new URL[]{classpath}, Thread.currentThread().getContextClassLoader())) {
            TProcessor processor = TProcessorParser.parseProcessor(props, classLoader);
            TTransportFactory transportFactory = TTransportFactoryParser.parseTransportFactory(props.getProperty("transport"));
            TProtocolFactory protocolFactory = TProtocolFactoryParser.parseProtocolFactory(props.getProperty("protocol"));
            ParameterizedPropertyParser.ParameterizedNode serverConf = ParameterizedPropertyParser.parse(props.getProperty("server"));
            TServerManager.serve(this.port, processor, transportFactory, protocolFactory, serverConf.getName(), serverConf.getAttrs());
        }
    }

    private static URI toURI(String path) {
        if (path == null || path.isEmpty()) {
            throw new IllegalStateException("classpath is empty!");
        }
        File file = new File(path);
        if (file.exists()) {
            return file.toURI();
        } else {
            return URI.create(path);
        }
    }

    public static void main(String... args) throws Exception {
        MockThriftServer provider = new MockThriftServer();
        JCommander.newBuilder().addObject(provider).build().parse(args);
        provider.serve();
    }
}
