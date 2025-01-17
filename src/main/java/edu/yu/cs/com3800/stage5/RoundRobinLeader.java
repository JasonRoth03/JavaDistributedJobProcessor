package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RoundRobinLeader extends Thread implements LoggingServer {
    private final PeerServer server;
    private final List<Long> peerIds;
    private final LinkedBlockingQueue<RoundRobinMessage> incomingMessages;
    private final HashMap<Long, Message> requestToMessage;
    private final ExecutorService exec;
    private final Logger logger;
    private final HashMap<Long, Message> requestIdtoCompletedMessage;

    public RoundRobinLeader(PeerServer server, List<Long> peerIds, LinkedBlockingQueue<RoundRobinMessage> incomingMessages) {
        this.server = server;
        this.peerIds = peerIds;
        this.incomingMessages = incomingMessages;
        this.requestToMessage = new HashMap<>();
        this.exec = Executors.newCachedThreadPool( new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                return t;
            }
        });

        try {
            this.logger = initializeLogging("RoundRobinLeader-ServerID-[" + server.getServerId() + "]-Port-[" + server.getAddress().getPort() + "]");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.requestIdtoCompletedMessage = new HashMap<>();
    }

    public void shutdown() {
        exec.shutdownNow();
        interrupt();
    }

    protected Map<Long, Message> getCompletedMessagesMap(){
        return requestIdtoCompletedMessage;
    }

    @Override
    public void run() {
        long reqId = 0;
        int count = 0;
        logger.fine("[" + Thread.currentThread().getName() + "] RoundRobinLeader started");
        while (!this.isInterrupted()) {
            if (count == peerIds.size()) {
                count = 0;
            }
            RoundRobinMessage m = incomingMessages.poll();
            if(m != null && m.getMessage().getMessageType() == Message.MessageType.WORK && requestIdtoCompletedMessage.containsKey(m.getMessage().getRequestID())){
                //This request ID was previously completed before a leader failed and was retransmitted to us when we became the new leader
                logger.fine("[" + Thread.currentThread().getName() + "] Not forwarding to worker, Returning previously completed message");

                m.getResponseQueue().offer(requestIdtoCompletedMessage.get(m.getMessage().getRequestID()));
            }else if (m != null && m.getMessage().getMessageType() == Message.MessageType.WORK) {
                //Check if this requestID maps to a completed message
                long worker = peerIds.get(count);
                if(server.isPeerDead(worker)){
                    logger.warning("[" + Thread.currentThread().getName() + "] Line 67: Worker:[" + worker+ "] is dead, Queuing for next available worker...");
                    incomingMessages.offer(m);
                    count++;
                    continue; // Dead Follower get a new one to run request
                }
                count++;
                logger.fine("[" + Thread.currentThread().getName() + "] RoundRobinLeader sending work to server ID: " + worker);
                //start up a thread to communicate synchronously with workers
                exec.submit(() -> {
                    logger.fine("[" + Thread.currentThread().getName() + "] RoundRobinLeader attempting to get worker address");
                    InetSocketAddress workerAddress = server.getPeerByID(worker);
                    if(server.isPeerDead(worker)){
                        logger.warning("[" + Thread.currentThread().getName() + "] Line 78: Worker:[" + worker + "] is dead, Queuing for next available worker...");
                        incomingMessages.offer(m);
                        return;
                    }
                    logger.fine("[" + Thread.currentThread().getName() + "] RoundRobinLeader acquired worker address: " + workerAddress);
                    try {
                        logger.fine("[" + Thread.currentThread().getName() + "] RoundRobinLeader attempting to connect to socket with worker");
                        Socket socket = new Socket(workerAddress.getHostName(), workerAddress.getPort() + 2);
                        socket.setKeepAlive(true);
                        logger.fine("[" + Thread.currentThread().getName() + "] RoundRobinLeader acquired the socket: " + socket.getLocalSocketAddress());
                        OutputStream outputStream = socket.getOutputStream();
                        logger.fine("[" + Thread.currentThread().getName() + "] RoundRobinLeader acquired output stream to worker");
                        outputStream.write(m.getMessage().getNetworkPayload());
                        outputStream.flush();
                        logger.fine("[" + Thread.currentThread().getName() + "] RoundRobinLeader wrote the message to worker node");
                        InputStream inputStream = socket.getInputStream();
                        logger.fine("[" + Thread.currentThread().getName() + "] RoundRobinLeader acquired input stream from worker");
                        //check if thread was interrupted bc of shutdown or other
                        if(isInterrupted()){
                            logger.fine("[" + Thread.currentThread().getName() + "] RoundRobinLeader interrupted after sending work to worker");
                            //close socket with worker
                            socket.close();
                            return;
                        }
                        //Check if worker failed since sending the response and receiving it
                        if(server.isPeerDead(worker)){
                            //Put the RoundRobinMessage back on our queue to be assigned a worker later in the loop
                            incomingMessages.offer(m);
                            socket.close();
                            logger.warning("[" + Thread.currentThread().getName() + "] Line 99 Worker:[" + worker + "] is dead, Queuing for next available worker...");
                            return;
                        }
                        if(isInterrupted()){
                            logger.fine("[" + Thread.currentThread().getName() + "] interrupted, before trying to read message from worker");
                            //close socket with worker
                            socket.close();
                            return;
                        }
                        byte[] networkPayload = Util.readAllBytesFromNetwork(inputStream);
                        while(networkPayload.length == 0 && !server.isPeerDead(worker)){
                            if(isInterrupted()){
                                logger.fine("[" + Thread.currentThread().getName() + "]interrupted after sending work to worker");
                                //close socket with worker
                                socket.close();
                                return;
                            }
                            networkPayload = Util.readAllBytesFromNetwork(inputStream);
                        }
                        if(server.isPeerDead(worker)){
                            incomingMessages.offer(m);
                            socket.close();
                            logger.warning("[" + Thread.currentThread().getName() + "] Line 151: Worker:[" + worker + "] is dead, Queuing for next available worker...");
                            return;
                        }
                        Message workerResponse = new Message(networkPayload);
                        logger.fine("[" + Thread.currentThread().getName() + "] RoundRobinLeader acquired worker response");
                        socket.close();
                        m.getResponseQueue().offer(workerResponse);
                    }catch (SocketException se){
                        logger.fine("[" + Thread.currentThread().getName() + "] RoundRobinLeader socket exception TRYING REQUEST WITH DIFFERENT FOLLOWER" );
                        incomingMessages.offer(m);
                    }catch (IOException e) {
                        //Problem with the connection to the worker
                        logger.fine("[" + Thread.currentThread().getName() + "] IOException: " + e.getMessage() + "\n TRYING REQUEST WITH DIFFERENT FOLLOWER");
                        incomingMessages.offer(m);
                    }
                });
            }else if(m != null && m.getMessage().getMessageType() == Message.MessageType.COMPLETED_WORK) {
                logger.fine("[" + Thread.currentThread().getName() + "] Got previously completed message");
                requestIdtoCompletedMessage.put(m.getMessage().getRequestID(), m.getMessage());
            } else if( m!= null && m.getMessage().getMessageType() != Message.MessageType.WORK) {
                logger.log(Level.SEVERE, "Message received by RoundRobin had a message type that was not WORK");
            }
        }
    }

    private Message forwardTo(Message m, InetSocketAddress address, long requestID) {
        return new Message(Message.MessageType.WORK, m.getMessageContents(), server.getAddress().getHostName(), server.getAddress().getPort(), address.getHostName(), address.getPort(), requestID);
    }
}
