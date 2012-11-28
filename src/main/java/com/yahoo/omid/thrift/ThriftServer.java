package com.yahoo.omid.thrift;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yahoo.omid.thrift.generated.TOmidService;

/**
 * ThriftServer - this class starts up a Thrift server which implements the Omid API specified in the Omid.thrift IDL
 * file.
 * 
 * This has been copied from HBase.
 */
public class ThriftServer {
    private static final Log log = LogFactory.getLog(ThriftServer.class);

    public static final String DEFAULT_LISTEN_PORT = "9090";

    public ThriftServer() {
    }

    private static void printUsage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Thrift", null, getOptions(),
                "To start the Thrift server run 'bin/hbase-daemon.sh start thrift2'\n"
                        + "To shutdown the thrift server run 'bin/hbase-daemon.sh stop thrift2' or"
                        + " send a kill signal to the thrift server pid", true);
    }

    private static Options getOptions() {
        Options options = new Options();
        options.addOption("b", "bind", true, "Address to bind the Thrift server to. [default: 0.0.0.0]");
        options.addOption("p", "port", true, "Port to bind to [default: " + DEFAULT_LISTEN_PORT + "]");
        options.addOption("f", "framed", false, "Use framed transport");
        options.addOption("c", "compact", false, "Use the compact protocol");
        options.addOption("h", "help", false, "Print help information");

        OptionGroup servers = new OptionGroup();
        servers.addOption(new Option("nonblocking", false,
                "Use the TNonblockingServer. This implies the framed transport."));
        servers.addOption(new Option("hsha", false, "Use the THsHaServer. This implies the framed transport."));
        servers.addOption(new Option("threadpool", false, "Use the TThreadPoolServer. This is the default."));
        options.addOptionGroup(servers);
        return options;
    }

    private static CommandLine parseArguments(Options options, String[] args) throws ParseException {
        CommandLineParser parser = new PosixParser();
        return parser.parse(options, args);
    }

    private static TProtocolFactory getTProtocolFactory(boolean isCompact) {
        if (isCompact) {
            log.debug("Using compact protocol");
            return new TCompactProtocol.Factory();
        } else {
            log.debug("Using binary protocol");
            return new TBinaryProtocol.Factory();
        }
    }

    private static TTransportFactory getTTransportFactory(boolean framed) {
        if (framed) {
            log.debug("Using framed transport");
            return new TFramedTransport.Factory();
        } else {
            return new TTransportFactory();
        }
    }

    /*
     * If bindValue is null, we don't bind.
     */
    private static InetSocketAddress bindToPort(String bindValue, int listenPort) throws UnknownHostException {
        try {
            if (bindValue == null) {
                return new InetSocketAddress(listenPort);
            } else {
                return new InetSocketAddress(InetAddress.getByName(bindValue), listenPort);
            }
        } catch (UnknownHostException e) {
            throw new RuntimeException("Could not bind to provided ip address", e);
        }
    }

    private static TServer getTNonBlockingServer(TProtocolFactory protocolFactory,
            TOmidService.Processor<TOmidService.Iface> processor, TTransportFactory transportFactory,
            InetSocketAddress inetSocketAddress) throws TTransportException {
        TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(inetSocketAddress);
        log.info("starting HBase Nonblocking Thrift server on " + inetSocketAddress.toString());
        TNonblockingServer.Args serverArgs = new TNonblockingServer.Args(serverTransport);
        serverArgs.processor(processor);
        serverArgs.transportFactory(transportFactory);
        serverArgs.protocolFactory(protocolFactory);
        return new TNonblockingServer(serverArgs);
    }

    private static TServer getTHsHaServer(TProtocolFactory protocolFactory,
            TOmidService.Processor<TOmidService.Iface> processor, TTransportFactory transportFactory,
            InetSocketAddress inetSocketAddress) throws TTransportException {
        TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(inetSocketAddress);
        log.info("starting HBase HsHA Thrift server on " + inetSocketAddress.toString());
        THsHaServer.Args serverArgs = new THsHaServer.Args(serverTransport);
        ExecutorService executorService = createExecutor(serverArgs.getWorkerThreads());
        serverArgs.executorService(executorService);
        serverArgs.processor(processor);
        serverArgs.transportFactory(transportFactory);
        serverArgs.protocolFactory(protocolFactory);
        return new THsHaServer(serverArgs);
    }

    private static ExecutorService createExecutor(int workerThreads) {
        ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
        tfb.setDaemon(true);
        tfb.setNameFormat("thrift2-worker-%d");
        return new ThreadPoolExecutor(workerThreads, workerThreads, Long.MAX_VALUE, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(), tfb.build());
    }

    private static TServer getTThreadPoolServer(TProtocolFactory protocolFactory,
            TOmidService.Processor<TOmidService.Iface> processor, TTransportFactory transportFactory,
            InetSocketAddress inetSocketAddress) throws TTransportException {
        TServerTransport serverTransport = new TServerSocket(inetSocketAddress);
        log.info("starting HBase ThreadPool Thrift server on " + inetSocketAddress.toString());
        TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport);
        serverArgs.processor(processor);
        serverArgs.transportFactory(transportFactory);
        serverArgs.protocolFactory(protocolFactory);
        return new TThreadPoolServer(serverArgs);
    }

    /**
     * Start up the Thrift2 server.
     * 
     * @param args
     */
    public static void main(String[] args) throws Exception {
        TServer server = null;
        System.out.println("Args: " + Arrays.toString(args));
        Options options = getOptions();
        try {
            CommandLine cmd = parseArguments(options, args);

            /**
             * This is to please both bin/hbase and bin/hbase-daemon. hbase-daemon provides "start" and "stop" arguments
             * hbase should print the help if no argument is provided
             */
            List<?> argList = cmd.getArgList();
            System.out.println("Arglist: " + argList);
            if (cmd.hasOption("help") || !argList.contains("start") || argList.contains("stop")) {
                printUsage();
                System.exit(1);
            }

            // Get port to bind to
            int listenPort = 0;
            try {
                listenPort = Integer.parseInt(cmd.getOptionValue("port", DEFAULT_LISTEN_PORT));
            } catch (NumberFormatException e) {
                throw new RuntimeException("Could not parse the value provided for the port option", e);
            }

            boolean nonblocking = cmd.hasOption("nonblocking");
            boolean hsha = cmd.hasOption("hsha");

            Configuration conf = HBaseConfiguration.create();

            // Construct correct ProtocolFactory
            TProtocolFactory protocolFactory = getTProtocolFactory(cmd.hasOption("compact"));
            TOmidService.Iface handler = ThriftServerHandler.newInstance(conf);
            TOmidService.Processor<TOmidService.Iface> processor = new TOmidService.Processor<TOmidService.Iface>(
                    handler);

            boolean framed = cmd.hasOption("framed") || nonblocking || hsha;
            TTransportFactory transportFactory = getTTransportFactory(framed);
            InetSocketAddress inetSocketAddress = bindToPort(cmd.getOptionValue("bind"), listenPort);

            if (nonblocking) {
                server = getTNonBlockingServer(protocolFactory, processor, transportFactory, inetSocketAddress);
            } else if (hsha) {
                server = getTHsHaServer(protocolFactory, processor, transportFactory, inetSocketAddress);
            } else {
                server = getTThreadPoolServer(protocolFactory, processor, transportFactory, inetSocketAddress);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            printUsage();
            System.exit(1);
        }
        server.serve();
    }
}
