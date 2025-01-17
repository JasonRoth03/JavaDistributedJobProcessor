package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.*;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PeerServerImpl extends Thread implements PeerServer, LoggingServer {

    protected Logger logger;
    protected final InetSocketAddress address;
    protected final int myPort;
    protected final LinkedBlockingQueue<Message> outgoingMessages;
    protected final LinkedBlockingQueue<Message> incomingMessages;
    protected final Long id;
    protected ServerState serverState;
    protected volatile boolean shutdown;
    protected long peerEpoch;
    protected volatile Vote currentLeader;
    protected final Map<Long, InetSocketAddress> peerIDtoAddress;
    protected UDPMessageSender senderWorker;
    protected UDPMessageReceiver receiverWorker;
    protected JavaRunnerFollower jrf;
    protected RoundRobinLeader rrl;
    protected int numberOfObservers;
    private final ExecutorService exec;
    private final int tcpPort;
    private final Long gatewayID;
    private final ConcurrentHashMap<Long, InetSocketAddress> failedPeerMap;
    protected final Gossiper gossiper;
    private final Logger gossipSummaryLogger;
    private Message followerCachedResponse;
    private HttpServer httpServer = null;
    private final int httpPort;
    private ServerSocket serverSocket = null;

    public PeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long, InetSocketAddress> peerIDtoAddress, Long gatewayID, int numberOfObservers) {
        this.address = new InetSocketAddress("localhost", myPort);
        this.serverState = ServerState.LOOKING;
        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();
        this.myPort = myPort;
        this.peerEpoch = peerEpoch;
        this.id = id;
        this.peerIDtoAddress = peerIDtoAddress;
        this.gatewayID = gatewayID;
        tcpPort = myPort + 2;
        try {
            this.logger = initializeLogging(PeerServerImpl.class.getName() + myPort);
            gossipSummaryLogger = initializeLogging("Summary-Gossip-ServerID:" + this.id);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.gossiper = new Gossiper(this, incomingMessages, gossipSummaryLogger);

        this.exec = Executors.newCachedThreadPool( new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                return t;
            }
        });

        this.numberOfObservers = numberOfObservers;

        failedPeerMap = new ConcurrentHashMap<>();

        this.httpPort = myPort + 3;  // Use UDP port + 3 for HTTP

        // Get log directory name when server starts
        LocalDateTime date = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-kk_mm");
        String logDirName = "logs-" + date.format(formatter);

        if(!Objects.equals(id, gatewayID)) {
            try {
                // Create and start HTTP server
                InetSocketAddress httpAddress = new InetSocketAddress(httpPort);
                httpServer = HttpServer.create(httpAddress, 50);

                // Add log endpoints
                String summaryPath = logDirName + File.separator + "Summary-Gossip-ServerID:" + id + "-Log.txt";
                String verbosePath = logDirName + File.separator + "Verbose-Gossip-ServerID:" + id + "-Log.txt";
                httpServer.createContext("/logs/summary", new LogFileHandler(summaryPath));
                httpServer.createContext("/logs/verbose", new LogFileHandler(verbosePath));

                httpServer.setExecutor(Executors.newCachedThreadPool(r -> {
                    Thread t = new Thread(r);
                    t.setDaemon(true);
                    return t;
                }));

                httpServer.start();

            } catch (IOException e) {
                throw new RuntimeException("Failed to create HTTP server", e);
            }
        }
    }

    @Override
    public void shutdown() {
        logger.log(Level.SEVERE, "ServerID[" + getServerId() + "]" + "Shutting down peer server");
        gossiper.shutdown();
        this.senderWorker.shutdown();
        this.receiverWorker.shutdown();
        exec.shutdownNow();
        if (this.serverState == ServerState.LEADING) rrl.shutdown();
        if (this.serverState == ServerState.FOLLOWING) jrf.shutdown();
        this.interrupt();
        this.shutdown = true;
        if (httpServer != null) {
            httpServer.stop(0);
        }
        if(serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Vote getCurrentLeader() {
        return currentLeader;
    }

    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        currentLeader = v;
    }

    @Override
    public void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException {
        Message m = new Message(type, messageContents, address.getHostName(), myPort, target.getHostName(), target.getPort());
        outgoingMessages.offer(m);
    }

    @Override
    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            String targetHostName = entry.getValue().getHostName();
            int targetPort = entry.getValue().getPort();
            outgoingMessages.offer(new Message(type, messageContents, address.getHostName(), myPort, targetHostName, targetPort));
        }
    }

    @Override
    public ServerState getPeerState() {
        return serverState;
    }

    @Override
    public void setPeerState(ServerState newState) {
        serverState = newState;
    }

    @Override
    public Long getServerId() {
        return id;
    }

    @Override
    public long getPeerEpoch() {
        return peerEpoch;
    }

    @Override
    public InetSocketAddress getAddress() {
        return address;
    }

    @Override
    public int getUdpPort() {
        return myPort;
    }

    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        return peerIDtoAddress.get(peerId);
    }

    @Override
    public int getQuorumSize() {
        int peersNotObservers = peerIDtoAddress.size() - numberOfObservers;
        return (peersNotObservers / 2) + 1;
    }

    @Override
    public void run() {
        //step 1: create and run thread that sends broadcast messages
        senderWorker = new UDPMessageSender(outgoingMessages, myPort);
        senderWorker.start();

        //step 2: create and run thread that listens for messages sent to this server
        try {
            receiverWorker = new UDPMessageReceiver(incomingMessages, address, myPort, this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        receiverWorker.start();

        LinkedBlockingQueue<RoundRobinMessage> roundRobinQueue = new LinkedBlockingQueue<>();

        // Starts paused so now is waiting to be resumed
        gossiper.start();

        //step 3: main server loop
        try {
            ServerState prevState = ServerState.LOOKING;
            //serverSocket = null;
            boolean justLeftElection = false;
            while (!this.isInterrupted()) {
                switch (getPeerState()) {
                    case LOOKING:
                        if (prevState == ServerState.LEADING) {//previous state was LEADING -> shutdown RoundRobinLeader thread
                            rrl.shutdown();
                        }
                        if(prevState != getPeerState()){
                            String switchStateMsg = "[" + getServerId() + "]: switching from [" + prevState + "] to [" + getPeerState() + "]";
                            gossipSummaryLogger.fine(switchStateMsg);
                            System.out.println(switchStateMsg);
                        }
                        gossiper.pauseGossip();
                        LeaderElection election = new LeaderElection(this, incomingMessages, logger);
                        gossiper.resumeGossip();
                        setCurrentLeader(election.lookForLeader());

                        logger.fine("[" + Thread.currentThread().getName() + "]" + "State: " + getPeerState().toString());
                        //If the prev state was a java runner follower
                        if(prevState == ServerState.FOLLOWING && getPeerState() == ServerState.LEADING){
                            // Get the java runners cached repsonse
                            byte[] cachedResponse = jrf.getCachedResponse();
                            if(cachedResponse != null){
                                followerCachedResponse = new Message(cachedResponse);
                            }
                            jrf.shutdown();
                        }
                        justLeftElection = true;
                        break;
                    case FOLLOWING:
                        if(justLeftElection){
                            String switchStateMsg = "[" + getServerId() + "]: switching from [LOOKING] to [" + getPeerState() + "]";
                            gossipSummaryLogger.fine(switchStateMsg);
                            System.out.println(switchStateMsg);
                            justLeftElection = false;
                        }
//                        if (prevState == ServerState.LEADING) {//previous state was LEADING -> shutdown RoundRobinLeader thread
//                            rrl.shutdown();
//                        }
                        if (prevState != ServerState.FOLLOWING) {//previous state was not FOLLOWING -> start JavaRunnerFollower thread
                            prevState = ServerState.FOLLOWING;
                            if(serverSocket == null){
                                serverSocket = new ServerSocket(getUdpPort() + 2);
                            }
                            jrf = new JavaRunnerFollower(this, serverSocket, incomingMessages, outgoingMessages);
                            jrf.setDaemon(true);
                            jrf.start();
                        }
                        break;
                    case LEADING:
                        if(justLeftElection){
                            String switchStateMsg = "[" + getServerId() + "]: switching from [Looking] to [" + getPeerState() + "]";
                            gossipSummaryLogger.fine(switchStateMsg);
                            System.out.println(switchStateMsg);
                            justLeftElection = false;
                        }

                        if (prevState != ServerState.LEADING) {//previous state was not LEADING -> start RoundRobinLeader thread
                            prevState = ServerState.LEADING;
                            ArrayList<Long> peerIDs = new ArrayList<>(peerIDtoAddress.keySet());
                            peerIDs.remove(gatewayID);
                            rrl = new RoundRobinLeader(this, peerIDs, roundRobinQueue);
                            rrl.setDaemon(true);
                            rrl.start();
                        }
                        if(followerCachedResponse != null){
                            rrl.getCompletedMessagesMap().put(followerCachedResponse.getRequestID(), followerCachedResponse);
                        }
                        try{
                            serverSocket = new ServerSocket(getUdpPort() + 2);
                        }catch (IOException e) {
                            logger.severe("[" + Thread.currentThread().getName() + "] Leader could not start server socket");
                            throw new RuntimeException(e);
                        }
                        //poll followers to see if they have any completed work and pass them to Round Robin leader
                        Set<Long> followerPeerIds = peerIDtoAddress.keySet();
                        followerPeerIds.remove(gatewayID);
                        for(long peerId : followerPeerIds){
                            //open socket to follower
                            try {
                                Socket socket = new Socket(peerIDtoAddress.get(peerId).getHostName(), peerIDtoAddress.get(peerId).getPort() + 2);
                                OutputStream outputStream = socket.getOutputStream();
                                byte[] content = "cache".getBytes();
                                Message getCacheMessage = new Message(Message.MessageType.GOSSIP, content, this.getAddress().getHostName(), this.getAddress().getPort(), peerIDtoAddress.get(peerId).getHostName(), peerIDtoAddress.get(peerId).getPort() + 2);
                                socket.getOutputStream().write(getCacheMessage.getNetworkPayload());
                                socket.getOutputStream().flush();
                                logger.fine("Sent request to Server [" + peerId + "] for any cached completed work");
                                byte[] networkBytes = Util.readAllBytesFromNetwork(socket.getInputStream());
                                if (networkBytes.length > 0) {
                                    //System.out.println(networkBytes.length);
                                    Message response = new Message(networkBytes);
                                    if (response.getMessageType() == Message.MessageType.COMPLETED_WORK) {
                                        rrl.getCompletedMessagesMap().put(response.getRequestID(), response);
                                    }
                                }
                            }catch (ConnectException e) {
                                //If we cannot open socket to follower ignore
                                logger.fine("Could not open socket connection to worker [" + peerId + "]");
                            }
                        }
                        while(!this.isInterrupted()){
                            Socket socket = null;
                            try {
                                socket = serverSocket.accept();
                            } catch (IOException e) {
                                //Most likely server socket closed by shutdown method and now it's trying to accept
                                logger.fine("[" + Thread.currentThread().getName() + "] Accept failed: " + e.getMessage());
                            }
                            if(this.isInterrupted()){
                                break;
                            }
                            logger.fine("[" + Thread.currentThread().getName() + "]" + "Connection from " + socket.getRemoteSocketAddress());
                           try{
                               exec.submit(new TCPServer(getUdpPort() + 2, socket, roundRobinQueue));
                           }catch (RejectedExecutionException e){
                               logger.fine("[" + Thread.currentThread().getName() + "]" + " Rejected executor submit because peerserver is shutting down");
                           }
                        }
                        if(serverSocket != null && !serverSocket.isClosed()){
                            serverSocket.close();
                        }
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + getPeerState());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void reportFailedPeer(long peerID) {
        if(peerID == gatewayID){
            return;
        }
        failedPeerMap.put(peerID, getPeerByID(peerID));
        //clean out dead peer from id to address map
        peerIDtoAddress.remove(peerID);
        String logMsg = "[" + getServerId() + "]: FAILED PEER [" + peerID + "] - REMOVED FROM ADDRESS MAP";
        logger.fine(logMsg);
        //If the leader failed
        //1. Increase Epoch number and Change server state to looking
        if(currentLeader != null && currentLeader.getProposedLeaderID() == peerID){
            currentLeader = null;
            peerEpoch += 1;
            setPeerState(ServerState.LOOKING);
        }
    }

    @Override
    public boolean isPeerDead(long peerID) {
        return failedPeerMap.containsKey(peerID);
    }

    @Override
    public boolean isPeerDead(InetSocketAddress address) {
        return failedPeerMap.containsValue(address);
    }

    // Private methods
    public long getRandomServerID(){
        Random random = new Random();
        List<Long> serverIDList = new ArrayList<>(peerIDtoAddress.keySet());
        return serverIDList.get(random.nextInt(serverIDList.size()));
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
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: PeerServerImpl <port> <server-id> <num-observers> <cluster-size>");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        long serverId = Long.parseLong(args[1]);
        int numObservers = Integer.parseInt(args[2]);
        int clusterSize = Integer.parseInt(args[3]);

        // Create peer map (including gateway)
        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        peerMap.put(0L, new InetSocketAddress("localhost", 8000)); // Gateway
        for (int i = 1; i < clusterSize; i++) {
            if (i != serverId) { // exclude self
                peerMap.put((long)i, new InetSocketAddress("localhost", 8000 + (i * 10)));
            }
        }

        PeerServerImpl server = new PeerServerImpl(port, 0, serverId, peerMap, 0L, numObservers);
        server.start();
    }
}
