package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;

import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class JavaRunnerFollower extends Thread implements LoggingServer {
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final PeerServerImpl server;
    private final Logger logger;
    private final ServerSocket serverSocket;
    private byte[] cachedResponse = null;

    public JavaRunnerFollower(PeerServerImpl server, ServerSocket serverSocket, LinkedBlockingQueue<Message> incomingMessages, LinkedBlockingQueue<Message> outgoingMessages) {
        this.server = server;
        this.incomingMessages = incomingMessages;
        this.outgoingMessages = outgoingMessages;
        this.serverSocket = serverSocket;

        try {
            this.logger = initializeLogging("JavaRunnerFollower-ServerID-[" + server.getServerId() + "]-Port-[" + server.getUdpPort() + "]");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public byte[] getCachedResponse() {
        return cachedResponse;
    }

    public void shutdown() {
        logger.fine("Shutting down JavaRunnerFollower");
        interrupt();
        try {
            logger.fine("Closing ServerSocket");
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close(); // This will break the accept() call
            }
        } catch (IOException e) {
            logger.warning("Error closing server socket during shutdown: " + e.getMessage());
        }
    }

    @Override
    public void run() {
        logger.fine("[" + Thread.currentThread().getName() + "] Running JavaRunnerFollower");
        //start tcp server
        logger.fine("[" + Thread.currentThread().getName() + "] starting tcp server");
        logger.fine("[" + Thread.currentThread().getName() + "] listening on port " + (server.getUdpPort() + 2));
        while (!this.isInterrupted()) {
            try {
                Socket socket = serverSocket.accept();
                logger.fine("[" + Thread.currentThread().getName() + "] accepted connection from " + socket.getRemoteSocketAddress());
                InputStream in = socket.getInputStream();
                logger.fine("[" + Thread.currentThread().getName() + "] Acquired input stream from " + socket.getRemoteSocketAddress());
                if(this.isInterrupted()) {
                    socket.close();
                    logger.fine("[" + Thread.currentThread().getName() + "] interrupted");
                    break;
                }
                logger.fine("[" + Thread.currentThread().getName() + "] starting read from input stream");
                Message m = new Message(Util.readAllBytesFromNetwork(in));
                logger.fine("[" + Thread.currentThread().getName() + "] received " + m);
                //check if this is a request for cached work from the leader
                //check if leader exists
                if(m != null && m.getMessageType() == Message.MessageType.GOSSIP){
                    logger.fine("[" + Thread.currentThread().getName() + "] received GOSSIP message signaling a cache request");
                    String content = new String(m.getMessageContents());
                    if(content.equals("cache")){
                        //send back a cached message if there is one
                        if(cachedResponse != null){
                            logger.fine("[" + Thread.currentThread().getName() + " Sending back Cached Message");
                            socket.getOutputStream().write(cachedResponse);
                            socket.getOutputStream().flush();
                        }else{
                            logger.fine("[" + Thread.currentThread().getName() + " No Cached Messages at the follower");
                            Message noCache;
                            try {
                                noCache = new Message(Message.MessageType.GOSSIP, new byte[10], server.getAddress().getHostName(), server.getAddress().getPort(), m.getSenderHost(), m.getSenderPort());
                            }catch (NullPointerException ne){
                                logger.fine("[" + Thread.currentThread().getName() + "] Null pointer ");
                                throw new NullPointerException();
                            }
                            socket.getOutputStream().write(noCache.getNetworkPayload());
                            socket.getOutputStream().flush();
                        }
                    }
                }
                if (m != null && m.getMessageType() == Message.MessageType.WORK) {
                    logger.fine("[" + Thread.currentThread().getName() + "] received code: " + new String(m.getMessageContents()));
//                    try {
//                        logger.info("[" + Thread.currentThread().getName() + "] Sleeping for 5 seconds");
//                        sleep(5000);
//                    } catch (InterruptedException e) {
//                        //interrupted exception, restart loop and let it see the interruption
//                        continue;
//                    }
                    InputStream inputStream = new ByteArrayInputStream(m.getMessageContents());
                    logger.fine("Created InputStream from message");
                    String response;
                    try {
                        logger.fine("Creating java runner");
                        JavaRunner jr = new JavaRunner();
                        response = jr.compileAndRun(inputStream);
                        logger.fine("Java runner response: " + response);
                        if (response == null) response = "null";
                    }catch (Exception e) {
                        FileOutputStream out;
                        try {
                            out = new FileOutputStream("error.txt");
                            PrintStream ps = new PrintStream(out);
                            e.printStackTrace(ps);
                            response = e.getMessage() + '\n' + Files.readString(Paths.get("error.txt"));
                        } catch (IOException ex) {
                            throw new RuntimeException(ex);
                        }
                        byte[] responseContents = createResponseMessage(m.getSenderHost(), m.getSenderPort(), response.getBytes(), m.getRequestID());

                        //set cached response to be our response
                        cachedResponse = responseContents;

                        if(this.isInterrupted()){
                            //interrupted exception, restart loop and let it see the interruption
                            continue;
                        }
                        OutputStream outputStream = socket.getOutputStream();
                        outputStream.write(responseContents);
                        outputStream.flush();
                        logger.fine("Writing error response back to Round robin leader");
                        throw new RuntimeException(e);
                    }
                    byte[] responseContents = createResponseMessage(m.getSenderHost(), m.getSenderPort(), response.getBytes(), m.getRequestID());
                    cachedResponse = responseContents;
                    if(this.isInterrupted()){
                        //interrupted exception, restart loop and let it see the interruption
                        continue;
                    }
                    OutputStream outputStream = socket.getOutputStream();
                    outputStream.write(responseContents);
                    outputStream.flush();
                    logger.fine("Writing response back to Round robin leader");
                }
            }catch(SocketException s){
                logger.fine("Socket exception: " + s.getMessage() );
            }catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private byte[] createResponseMessage(String receiverHost, int receiverPort, byte[] contents, long requestID) {
        Message m = new Message(Message.MessageType.COMPLETED_WORK, contents, server.getAddress().getHostName(), server.getAddress().getPort(), receiverHost, receiverPort, requestID);
        return m.getNetworkPayload();
    }

    private void sendResponse(Message m) {
        outgoingMessages.offer(m);
    }
}
