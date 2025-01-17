package edu.yu.cs.com3800;

import edu.yu.cs.com3800.stage5.PeerServerImpl;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class Gossiper extends Thread implements LoggingServer {
    static final int GOSSIP = 2000;
    static final int FAIL = GOSSIP * 16;
    static final int CLEANUP = FAIL * 2;

    private final PeerServerImpl peerServer;
    private final LinkedBlockingQueue<Message> incomingGossips;
    private final Map<Long, GossipMessage> gossipData = new ConcurrentHashMap<>();
    private long lastGossipTime = 0;
    private Thread acceptGossipThread;
    private final Set<Long> failedServerIds = new HashSet<>();
    private final Set<InetSocketAddress> failedServerAddresses = new HashSet<>();
    private final Logger verboseGossipLogger;
    private final Logger summaryLogger;
    private volatile boolean paused = true;

    public Gossiper(PeerServerImpl peerServer, LinkedBlockingQueue<Message> incomingGossips, Logger summaryLogger) {
        this.peerServer = peerServer;
        this.incomingGossips = incomingGossips;
        // Put our servers heartbeat data as initial gossip data
        gossipData.put(peerServer.getServerId(), new GossipMessage(peerServer.getServerId(), 0, System.currentTimeMillis()));

        try {
            verboseGossipLogger = initializeLogging("Verbose-Gossip-ServerID:" + peerServer.getServerId());
            this.summaryLogger = summaryLogger;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        //send off initial gossip message
        //startAcceptGossipsThread(incomingGossips);
        while (!this.isInterrupted()) {
            if(!paused){
                if (System.currentTimeMillis() - lastGossipTime >= (GOSSIP)) {
                    sendGossip();
                    failAndCleanupServers();
                }else{
                    failAndCleanupServers();
                }
            }else{
                updateMyGossip();
            }
        }
        summaryLogger.severe("Exiting Gossiper.run()");
        //Interrupted
    }

    public void pauseGossip(){
        shutdownAcceptGossipThread();
        this.paused = true;

    }

    public void resumeGossip(){
        for(GossipMessage gossipMessage : gossipData.values()){
            gossipMessage.setTime(System.currentTimeMillis());
        }
        startAcceptGossipsThread(incomingGossips);
        this.paused = false;
    }

    public void shutdown(){
        shutdownAcceptGossipThread();
        String logMsg = "[" + peerServer.getServerId() + "]: Shutting down Gossip Server";
        summaryLogger.fine(logMsg);
        this.interrupt();
    }

    private void failAndCleanupServers(){
        for(long serverId : gossipData.keySet()){
            // If its already marked failed by the gossiper && the time to wait for cleanup has passed -> remove failed server from data structures including gossip tables
            if(failedServerIds.contains(serverId) && System.currentTimeMillis() - gossipData.get(serverId).getTime() >= CLEANUP){
                String logMsg = "[" + peerServer.getServerId() + "]: CLEANUP period passed [" + serverId + "] - Cleaning up data";
                failedServerIds.remove(serverId);
                summaryLogger.fine(logMsg);
                gossipData.remove(serverId); // Remove gossip data from our tables

            // If PeerServer has not already failed this server AND If it is not marked failed by the gossiper but the time elapses constitutes a failure then fail the node and report
            }else if(!peerServer.isPeerDead(serverId) && !failedServerIds.contains(serverId) && System.currentTimeMillis() - gossipData.get(serverId).getTime() >= FAIL){
                String logMsg = "[" + peerServer.getServerId() + "]: no heartbeat from server [" + serverId + "] - SERVER FAILED";
                summaryLogger.fine(logMsg);
                System.out.println(logMsg);
                failedServerAddresses.add(peerServer.getPeerByID(serverId));
                failedServerIds.add(serverId);
                peerServer.reportFailedPeer(serverId);
            }
        }
    }

    private void shutdownAcceptGossipThread() {
        if(acceptGossipThread != null && !acceptGossipThread.isInterrupted()){
            acceptGossipThread.interrupt();
        }
    }

    private void startAcceptGossipsThread(LinkedBlockingQueue<Message> gossips) {
        acceptGossipThread = new Thread(() -> {
                long lastReceivedTime = 0;
                while (!acceptGossipThread.isInterrupted()) {
                    if(System.currentTimeMillis() - lastReceivedTime >= GOSSIP){
                        while(!gossips.isEmpty()){
                            Message gossipMessage = null;
                            try {
                                gossipMessage = gossips.take();
                            } catch (InterruptedException e) {
                                interrupt();
                                break;
                            }
                            InetSocketAddress sender = new InetSocketAddress(gossipMessage.getSenderHost(), gossipMessage.getSenderPort());
                            // Don't accept gossip messages from failed servers

                            if(gossipMessage.getMessageType() == Message.MessageType.GOSSIP && !failedServerAddresses.contains(sender)) {
                                Map<Long, GossipMessage> incomingGossips = deserializeGossip(gossipMessage.getMessageContents());
                                verboseGossipLogger.fine( "Time: " + System.currentTimeMillis() + ", Received gossip from: [" + incomingGossips.values().iterator().next().sourceServerId + "] -"  + incomingGossips);
                                mergeGossipData(incomingGossips);
                            }else if(gossipMessage.getMessageType() != Message.MessageType.GOSSIP){
                                gossips.offer(gossipMessage);
                            }
                        }
                        lastReceivedTime = System.currentTimeMillis();
                    }
                    
                }
        });
        acceptGossipThread.start();
    }

    private void mergeGossipData(Map<Long, GossipMessage> incomingGossipData) {
        long sourceId = incomingGossipData.values().iterator().next().sourceServerId;
        for(Long key : incomingGossipData.keySet()){
            GossipMessage gossipMessage = incomingGossipData.get(key);
            // We are seeing this nodes gossip data for the first time
            if(!gossipData.containsKey(key)){
                gossipMessage.setSourceServerId(peerServer.getServerId());
                gossipMessage.setTime(System.currentTimeMillis());
                gossipData.put(key, gossipMessage);
                summaryLogger.fine("[" + peerServer.getServerId() + "]: updated [" + key + "]'s heartbeat sequence to [" + gossipMessage.getHeartbeatCount() + "] base on message from [" + sourceId + "] at node time [" + gossipMessage.getTime() + "]");
            }
            // We have seen this node previously and there is a new heartbeat count than ours
            else if(gossipData.containsKey(key) && gossipData.get(key).getHeartbeatCount() < incomingGossipData.get(key).getHeartbeatCount()){
                //Check if the serverID we are updating has been marked as failed & skip update if it is failed
                if(failedServerIds.contains(key)){
                    continue;
                }
                gossipMessage.setSourceServerId(peerServer.getServerId());
                gossipMessage.setTime(System.currentTimeMillis());
                gossipData.put(key, gossipMessage);
                summaryLogger.fine("[" + peerServer.getServerId() + "]: updated [" + key + "]'s heartbeat sequence to [" + gossipMessage.getHeartbeatCount() + "] base on message from [" + sourceId + "] at node time [" + gossipMessage.getTime() + "]");
            }
        }
    }

    private void sendGossip(){
        try{
            updateMyGossip();
            byte[] content = serializeGossip(gossipData);
            long randPeerServerId = peerServer.getRandomServerID();
            peerServer.sendMessage(Message.MessageType.GOSSIP, content, peerServer.getPeerByID(randPeerServerId));
            lastGossipTime = System.currentTimeMillis();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private byte[] serializeGossip(Map<Long, GossipMessage> gossipData) {
        try{
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
            objectOutputStream.writeObject(gossipData);
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private ConcurrentHashMap<Long, GossipMessage> deserializeGossip(byte[] data) {
        try{
            ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
            ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
            return (ConcurrentHashMap<Long, GossipMessage>) objectInputStream.readObject();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void updateMyGossip() {
        long heartbeat = gossipData.get(peerServer.getServerId()).getHeartbeatCount();
        gossipData.get(peerServer.getServerId()).setHeartbeatCount(heartbeat + 1);
        gossipData.get(peerServer.getServerId()).setTime(System.currentTimeMillis());
    }

    private static class GossipMessage implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        private long heartbeatCount;
        private long time;
        private long sourceServerId;

        public GossipMessage(long sourceServerId, long heartbeatCount, long time){
            this.heartbeatCount = heartbeatCount;
            this.time = time;
            this.sourceServerId = sourceServerId;
        }

        public long getSourceServerId() {
            return sourceServerId;
        }

        public void setSourceServerId(long sourceServerId) {
            this.sourceServerId = sourceServerId;
        }

        public long getHeartbeatCount() {
            return heartbeatCount;
        }

        public void setHeartbeatCount(long heartbeatCount) {
            this.heartbeatCount = heartbeatCount;
        }

        public long getTime() {
            return time;
        }

        public void setTime(long time) {
            this.time = time;
        }

        @Override
        public String toString() {
            return "GossipMessage{" +
                    "heartbeatCount=" + heartbeatCount +
                    ", time=" + time +
                    '}';
        }
    }
}
