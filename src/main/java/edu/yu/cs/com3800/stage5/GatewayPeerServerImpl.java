package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.stage5.PeerServerImpl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class GatewayPeerServerImpl extends PeerServerImpl {

    public GatewayPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long, InetSocketAddress> peerIDtoAddress, Long gatewayID, int numberOfObservers) {
        super(myPort, peerEpoch, id, peerIDtoAddress, gatewayID, numberOfObservers);
        setPeerState(ServerState.OBSERVER);
        try {
            logger = initializeLogging(PeerServerImpl.class.getName() + myPort);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


    @Override
    public void setPeerState(ServerState newState) {
        //Set state to be OBSERVER IFF the state is not already an OBSERVER
        if(newState == ServerState.OBSERVER && serverState != ServerState.OBSERVER){
            serverState = newState;
        }
        //Ignore states that are not OBSERVER
    }

    @Override
    public int getQuorumSize() {
        int peersNotObservers = peerIDtoAddress.size() - (numberOfObservers - 1);
        return (peersNotObservers / 2) + 1;
    }


    @Override
    public void run(){
        //Start the gossiper
        gossiper.start();

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

        logger.fine("GatewayPeerServer starting state: " + getPeerState().toString());
        while(!this.isInterrupted()){
            if(currentLeader == null || isPeerDead(currentLeader.getProposedLeaderID())){
                gossiper.pauseGossip();
                //start leader election, set leader to the election winner
                LeaderElection election = new LeaderElection(this, incomingMessages, logger);
                try {
                    logger.fine("[ " + Thread.currentThread().getName() + "]" + " Starting leader election");
                    setCurrentLeader(election.lookForLeader());
                } catch (IOException e) {
                    logger.severe("[ " + Thread.currentThread().getName() + "]" + " setting current leader threw an exception");
                    throw new RuntimeException(e);
                }
                logger.fine("Inside GatewayPeerServerImpl - [" + Thread.currentThread().getName() + "]" + "State: " + getPeerState().toString());
                gossiper.resumeGossip();
            }
        }
    }

}
