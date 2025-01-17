package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class GatewayServer extends Thread implements LoggingServer{
    // 1. Create an HTTP server on whatever port is passed into the constructor
    // 2. Keep track of node that is leader & send all client requests to leader over TCP connection
    // 3. Is a peer in the cluster, but OBSERVER only -> Does NOT vote, Only observes, so it knows which node is the leader
    // 4. Must never change state to anything but OBSERVER
    // 5. will have a number of threads running:
    //      - HttpServer to accept client requests
    //      - GatewayPeerServerImpl (subclass of PeerServerImpl) that can only be an OBSERVER
    //      -
    private final int httpPort;
    private final int peerPort;
    private final long peerEpoch;
    private final Long serverID;
    private final ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress;
    private int numberOfObservers;
    private final HttpServer server;
    private final ConcurrentHashMap<Integer, byte[]> cache;
    private final int tcpPort;
    private final InetSocketAddress httpAddress;
    private final Logger logger;
    private FileHandler fh;
    private GatewayPeerServerImpl gatewayPeerServer;
    private final AtomicLong counter = new AtomicLong(1);
    private volatile boolean hasLeader = false;

    public GatewayServer(int httpPort, int peerPort, long peerEpoch, Long serverID, ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress, int numberOfObservers) throws IOException {
        this.httpPort = httpPort;
        this.peerPort = peerPort;
        this.peerEpoch = peerEpoch;
        this.serverID = serverID;
        this.peerIDtoAddress = peerIDtoAddress;
        this.tcpPort = peerPort + 2;

        //init cache
        cache = new ConcurrentHashMap<>();

        httpAddress = new InetSocketAddress(httpPort);
        //Start Http Server for receiving client connections
        server = HttpServer.create(httpAddress,  100);
        server.createContext("/compileandrun", new clientHandler());

        server.createContext("/cluster-info", new clusterInfoHandler());

        // Get log directory name when server starts
        LocalDateTime date = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-kk_mm");
        String logDirName = "logs-" + date.format(formatter);

        // Add endpoints for log files using the stored directory name
        String summaryPath = logDirName + File.separator + "Summary-Gossip-ServerID:" + serverID + "-Log.txt";
        String verbosePath = logDirName + File.separator + "Verbose-Gossip-ServerID:" + serverID + "-Log.txt";

        server.createContext("/logs/summary", new LogFileHandler(summaryPath));
        server.createContext("/logs/verbose", new LogFileHandler(verbosePath));

        this.logger = initializeLogging(GatewayServer.class.getName() + "-httpPort-" + httpPort);

        server.setExecutor(Executors.newCachedThreadPool(  new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                return t;
            }
        }));
    }

    public void shutdown() {
        interrupt();
        this.server.stop(0);
        gatewayPeerServer.shutdown();
    }

    public GatewayPeerServerImpl getPeerServer() {
        return this.gatewayPeerServer;
    }


    @Override
    public void run() {
        server.start();
        gatewayPeerServer = new GatewayPeerServerImpl(peerPort, peerEpoch, serverID, peerIDtoAddress, serverID, numberOfObservers);
        gatewayPeerServer.start();
        logger.fine("Gateway Server started");
        while(!this.isInterrupted()){
            try {
                sleep(100);
            } catch (InterruptedException e) {
                interrupt();
            }
        }
    }

    private static class LogFileHandler implements HttpHandler {
        private final String logFilePath;

        public LogFileHandler(String logFilePath) {
            this.logFilePath = logFilePath;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!exchange.getRequestMethod().equals("GET")) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            File logFile = new File(logFilePath);
            if (!logFile.exists()) {
                String response = "Log file not found";
                exchange.sendResponseHeaders(404, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
                return;
            }

            byte[] logContent = Files.readAllBytes(logFile.toPath());
            exchange.getResponseHeaders().set("Content-Type", "text/plain");
            exchange.sendResponseHeaders(200, logContent.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(logContent);
                os.flush();
                os.close();
            }
        }
    }


    class clusterInfoHandler implements HttpHandler {

        /**
         * Handle the given request and generate an appropriate response.
         * See {@link HttpExchange} for a description of the steps
         * involved in handling an exchange.
         *
         * @param exchange the exchange containing the request from the
         *                 client and used to send the response
         * @throws NullPointerException if exchange is {@code null}
         * @throws IOException          if an I/O error occurs
         */
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            StringBuilder response = new StringBuilder();

            try{
                if(gatewayPeerServer.currentLeader != null){
                    long leaderId = gatewayPeerServer.currentLeader.getProposedLeaderID();
                    for(long key : gatewayPeerServer.peerIDtoAddress.keySet()){
                        if(key == leaderId){
                            response.append(String.format("Server %d - LEADER\n", key));
                        }else if(key == gatewayPeerServer.getServerId()){
                            response.append(String.format("Server %d - OBSERVER\n", key));
                        }
                        else{
                            response.append(String.format("Server %d - FOLLOWER\n", key));
                        }
                    }
                }else{
                    response.append("Negative\n");
                }
            }catch (Exception e){
                logger.fine("Cluster info failed: " + e.getMessage());
                response.append("Negative\n");
            }


            byte[] responseBytes = response.toString().getBytes();
            exchange.getResponseHeaders().set("Content-Type", "text/plain");
            exchange.sendResponseHeaders(200, responseBytes.length);

            OutputStream os = exchange.getResponseBody();
            os.write(responseBytes);
            os.flush();
            os.close();
        }
    }

    class clientHandler implements HttpHandler {

        //Handles regular use of system
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            boolean requestSuccessful = false;
            logger.fine("[" + Thread.currentThread().getName() + "] Received HTTP request");
            logger.fine("Request Method: " + exchange.getRequestMethod());
            logger.fine("Request URI: " + exchange.getRequestURI());
            if(exchange.getRequestMethod().equalsIgnoreCase("POST")){
                Headers requestHeaders = exchange.getRequestHeaders();
                if(requestHeaders.containsKey("Content-Type") && requestHeaders.get("Content-Type").contains("text/x-java-source")){
                    byte[] requestBody = exchange.getRequestBody().readAllBytes();
                    int requestBodyHash = new String(requestBody).hashCode();
                    logger.fine("[" + Thread.currentThread().getName() + "] request body: " + new String(requestBody));
                    byte[] responseContent;
                    if(cache.containsKey(requestBodyHash)){
                        logger.fine("[" + Thread.currentThread().getName() + "] Cache Hit! sending response from cache...");// Use the cached response
                        responseContent = cache.get(requestBodyHash);
                        try{
                            exchange.getResponseHeaders().set("Cached-Response", "true");
                            exchange.sendResponseHeaders(200, responseContent.length);
                            OutputStream os = exchange.getResponseBody();
                            os.write(responseContent);
                            os.flush();
                            os.close();
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                    }else{ //Forward client request to Master/Leader and then cache response
                        //Check if leader has failed
                        long requestID = getRequestID();
                        logger.fine("[" + Thread.currentThread().getName() + "] requestID: " + requestID);
                        logger.fine("[" + Thread.currentThread().getName() + "] Checking if we have a leader");
                        while(!requestSuccessful){
                            logger.fine("About to assign leaderID");
                            logger.fine("About to enter while loop for dead leader");
                            while(gatewayPeerServer.currentLeader == null || gatewayPeerServer.isPeerDead(gatewayPeerServer.currentLeader.getProposedLeaderID())){
                                //We have no leader so wait to send request to a leader
                                logger.fine("[" + Thread.currentThread().getName() + "] Waiting a leader");
                                try {
                                    sleep(100);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                            long currentLeader = gatewayPeerServer.currentLeader.getProposedLeaderID();
                            Message request = createMessage(requestBody, requestID, currentLeader);
                            logger.fine("[" + Thread.currentThread().getName() + "] Message: " + request);
                            try{
                                logger.fine("[" + Thread.currentThread().getName() + "] attempting to create socket with Leader");
                                if(gatewayPeerServer.isPeerDead(currentLeader)){
                                    logger.fine("[" + Thread.currentThread().getName() + "] leader is dead according to peerserver");
                                    continue;
                                }
                                Socket socket = new Socket(peerIDtoAddress.get(currentLeader).getHostName(), peerIDtoAddress.get(currentLeader).getPort() + 2);
                                logger.fine("[" + Thread.currentThread().getName() + "] created socket with Leader");
                                OutputStream outputStream = socket.getOutputStream();
                                outputStream.write(request.getNetworkPayload());
                                outputStream.flush();
                                logger.fine("[" + Thread.currentThread().getName() + "] forwarded request to Leader");
                                InputStream inputStream = socket.getInputStream();
                                logger.fine("[" + Thread.currentThread().getName() + "] Got input stream from leader");
                                byte[] responseBytes = new byte[inputStream.available()];
                                while(!gatewayPeerServer.isPeerDead(currentLeader)){
                                    responseBytes = Util.readAllBytesFromNetwork(inputStream);
                                    logger.fine("[" + Thread.currentThread().getName() + "] Got response: bytes: (" + responseBytes.length + ") , " + new String(responseBytes));
                                    if(responseBytes.length == 0) continue;
                                    else{
                                        break;
                                    }
                                }
                                if(gatewayPeerServer.isPeerDead(currentLeader)){
                                    //No response coming back because the leader is dead
                                    //1. Close the socket to the dead leader
                                    logger.fine("[" + Thread.currentThread().getName() + "] Leader [" + currentLeader +  "] is dead");
                                    socket.close();
                                    //2. restart loop to try and send request again
                                    continue;
                                }
                                //There is some response received
                                if(responseBytes.length > 0){
                                    Message leaderResponse = new Message(responseBytes);
                                    logger.fine("[" + Thread.currentThread().getName() + "] received response from Leader");
                                    socket.close();
                                    cache.put(requestBodyHash, leaderResponse.getMessageContents());
                                    logger.fine("[" + Thread.currentThread().getName() + "] Cached previously not seen results");
                                    responseContent = leaderResponse.getMessageContents();
                                    exchange.getResponseHeaders().set("Cached-Response", "false");
                                    exchange.sendResponseHeaders(200, responseContent.length);
                                    OutputStream os = exchange.getResponseBody();
                                    os.write(responseContent);
                                    os.flush();
                                    os.close();
                                }
                                requestSuccessful = true;
                                logger.fine("[" + Thread.currentThread().getName() + "] request successful");
                            }catch (ConnectException ce){
                                //unable to connect with leader (Maybe dead but gossips will tell), Continue
                                logger.fine("[" + Thread.currentThread().getName() + "] Connection Exception: Unable to connect to leader");
                                try {
                                    sleep(200);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            }catch (SocketException e){
                                //unable to connect with leader (Maybe dead but gossips will tell), Continue
                                logger.fine("[" + Thread.currentThread().getName() + "] SocketException: " + e);
                                try {
                                    sleep(200);
                                } catch (InterruptedException ie) {
                                    throw new RuntimeException(e);
                                }
                            } catch (Exception e){
                                logger.severe("[" + Thread.currentThread().getName() + "] Error: " + e);
                                e.printStackTrace();
                            }
                        }
                    }
                }else{ //Incorrect content-type
                    String response = "Content-Type is not supported, must be \"text/x-java-source\"";
                    exchange.getResponseHeaders().set("Cached-Response", "false");
                    exchange.sendResponseHeaders(400, response.length());
                    OutputStream os = exchange.getResponseBody();
                    os.write(response.getBytes());
                }
            }else{
                logger.fine("[" + Thread.currentThread().getName() + "] HTTP request method not supported");
                exchange.getResponseHeaders().set("Cached-Response", "false");
                exchange.sendResponseHeaders(405, -1);
            }
        }

        private Message createMessage(byte[] contents, long requestId, long leaderId){
            logger.fine("[" + Thread.currentThread().getName() + "] Attempting to create message");
            String leaderHost = peerIDtoAddress.get(leaderId).getHostName();
            logger.fine("[" + Thread.currentThread().getName() + "] leader host: " + leaderHost);
            int leaderPort = peerIDtoAddress.get(leaderId).getPort();
            logger.fine("[" + Thread.currentThread().getName() + "] leader port: " + leaderPort);
            logger.fine("[" + Thread.currentThread().getName() + "] Content length: " + (contents != null ? contents.length : "null"));
            logger.fine("[" + Thread.currentThread().getName() + "] Content: " + (contents != null ? new String(contents) : "null"));
            logger.fine("[" + Thread.currentThread().getName() + "] TCP Port: " + tcpPort);
            logger.fine("[" + Thread.currentThread().getName() + "] HTTP Address: " + httpAddress.getHostName());

            Message msg = new Message(Message.MessageType.WORK, contents, httpAddress.getHostName(), tcpPort, leaderHost, leaderPort, requestId);
            logger.fine("[" + Thread.currentThread().getName() + "] Created message: " + msg);
            return msg;
        }
    }

    private long getRequestID(){
        return counter.getAndIncrement();
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.out.println("Usage: GatewayServer <http-port> <udp-port> <cluster-size>");
            System.exit(1);
        }

        int httpPort = Integer.parseInt(args[0]);
        int udpPort = Integer.parseInt(args[1]);
        int clusterSize = Integer.parseInt(args[2]);

        // Create peer map (excluding gateway)
        ConcurrentHashMap<Long, InetSocketAddress> peerMap = new ConcurrentHashMap<>();
        for (long i = 1; i < clusterSize; i++) {
            peerMap.put(i, new InetSocketAddress("localhost", 8000 + ((int)i * 10)));
        }

        GatewayServer gateway = new GatewayServer(httpPort, udpPort, 0, 0L, peerMap, 1);
        gateway.start();
    }
}
