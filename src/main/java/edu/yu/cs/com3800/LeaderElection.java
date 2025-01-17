package edu.yu.cs.com3800;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * We are implementing a simplified version of the election algorithm. For the complete version which covers all possible scenarios, see https://github.com/apache/zookeeper/blob/90f8d835e065ea12dddd8ed9ca20872a4412c78a/zookeeper-server/src/main/java/org/apache/zookeeper/server/quorum/FastLeaderElection.java#L913
 */
public class LeaderElection {
    /**
     * time to wait once we believe we've reached the end of leader election.
     */
    private final static int finalizeWait = 2800;

    /**
     * Upper bound on the amount of time between two consecutive notification checks.
     * This impacts the amount of time to get the system up again after long partitions. Currently, 30 seconds.
     */
    private final static int maxNotificationInterval = 30000;
    private final Logger logger;
    private static FileHandler fh;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final PeerServer server;
    private final HashMap<Long, ElectionNotification> votesReceived = new HashMap<>();
    private Vote myVote;
    private long proposedLeader;


    public LeaderElection(PeerServer server, LinkedBlockingQueue<Message> incomingMessages, Logger logger) {
        this.server = server;
        this.incomingMessages = incomingMessages;
        this.logger = logger;
        myVote = new Vote(server.getServerId(), server.getPeerEpoch());
        proposedLeader = server.getServerId();
    }

    public static byte[] buildMsgContent(ElectionNotification notification) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 3 + Character.BYTES);

        // Write the long for the proposed leader ID
        buffer.putLong(notification.getProposedLeaderID());

        // Write the state as a char
        buffer.putChar(notification.getState().getChar()); // Assuming 'state' is an enum with a single-char name

        // Write the long for the sender ID
        buffer.putLong(notification.getSenderID());

        // Write the long for the peer epoch
        buffer.putLong(notification.getPeerEpoch());

        // Convert to a byte array
        return buffer.array();
    }

    public static ElectionNotification getNotificationFromMessage(Message m) {
        ByteBuffer msgBytes = ByteBuffer.wrap(m.getMessageContents());
        long leader = msgBytes.getLong();
        char stateChar = msgBytes.getChar();
        long senderID = msgBytes.getLong();
        long peerEpoch = msgBytes.getLong();

        return new ElectionNotification(leader, PeerServer.ServerState.getServerState(stateChar), senderID, peerEpoch);
    }

    /**
     * Note that the logic in the comments below does NOT cover every last "technical" detail you will need to address to implement the election algorithm.
     * How you store all the relevant state, etc., are details you will need to work out.
     *
     * @return the elected leader
     */
    public synchronized Vote lookForLeader() {
        logger.log(Level.FINE, "[" + Thread.currentThread().getName() + "]" + "LeaderElection lookForLeader");
        try {
            //send initial notifications to get things started
            sendNotifications();
            logger.fine("[" + Thread.currentThread().getName() + "]" + "Sent Initial Notifications");
            //Loop in which we exchange notifications with other servers until we find a leader
            int currentInterval = 200;
            while (true) {
                boolean ignoreMessage = false;
                //Remove next notification from queue
                Message m = incomingMessages.poll(currentInterval, TimeUnit.MILLISECONDS);
                //If no notifications received...
                if (m == null || m.getMessageType() == Message.MessageType.GOSSIP) {
                    //...resend notifications to prompt a reply from others
                    sendNotifications();
                    //...use exponential back-off when notifications not received but no longer than maxNotificationInterval...
                    if (currentInterval * 2 <= maxNotificationInterval) {
                        currentInterval *= 2;
                    } else {
                        currentInterval = maxNotificationInterval;
                    }
                    continue;
                }
                //If we did get a message...
                ElectionNotification notification = getNotificationFromMessage(m);
                logger.fine("[" + Thread.currentThread().getName() + "]" + "Received Message from ID: " + notification.getSenderID());
                //...if it's for an earlier epoch, or from an observer, ignore it.
                if (notification.getPeerEpoch() < server.getPeerEpoch() || notification.getState() == PeerServer.ServerState.OBSERVER ) {
                    ignoreMessage = true;
                    logger.fine("[" + Thread.currentThread().getName() + "]" + "Ignoring Message from lower epoch or OBSERVER: " + m);
                } else if (supersedesCurrentVote(notification.getProposedLeaderID(), notification.getPeerEpoch())) {
                    //...if the received message has a vote for a leader which supersedes mine, change my vote (and send notifications to all other voters about my new vote).
                    myVote = new Vote(notification.getProposedLeaderID(), notification.getPeerEpoch());
                    proposedLeader = myVote.getProposedLeaderID();
                    sendNotifications();
                    logger.fine("[" + Thread.currentThread().getName() + "]" + "Sent Proposed Leader: " + proposedLeader);
                }
                //(Be sure to keep track of the votes I received and who I received them from.)
                if (!ignoreMessage) {
                    votesReceived.put(notification.getSenderID(), notification);
                }

                ElectionNotification myNotification = new ElectionNotification(myVote.getProposedLeaderID(), server.getPeerState(), server.getServerId(), myVote.getPeerEpoch());
                votesReceived.put(server.getServerId(), myNotification);
                //If I have enough votes to declare my currently proposed leader as the leader...
                if (haveEnoughVotes(votesReceived, myVote)) {
                    //…do a last check to see if there are any new votes for a higher ranked possible leader. If there are, continue in my election "while" loop.
                    Thread.sleep(finalizeWait);
                    m = incomingMessages.peek();
                    while (m != null) {
                        notification = getNotificationFromMessage(m);
                        if (notification.getState() != PeerServer.ServerState.OBSERVER && supersedesCurrentVote(notification.getProposedLeaderID(), notification.getPeerEpoch())) {
                            break;
                        } else {
                            incomingMessages.poll();
                            m = incomingMessages.peek();
                        }
                    }
                    if (m != null) continue;
                    //If there are no new relevant message from the reception queue, set my own state to either LEADING or FOLLOWING and RETURN the elected leader.

                    return acceptElectionWinner(votesReceived.get(server.getServerId()));
                }

            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "[" + server.getServerId() + "]" + "Exception occurred during election; election canceled", e);
        }
        return null;
    }

    public void sendNotifications() {
        ElectionNotification notification = new ElectionNotification(myVote.getProposedLeaderID(), server.getPeerState(), server.getServerId(), server.getPeerEpoch());
        server.sendBroadcast(Message.MessageType.ELECTION, buildMsgContent(notification));
    }

    private Vote acceptElectionWinner(ElectionNotification n) {
        //set my state to either LEADING or FOLLOWING
        if (n.getProposedLeaderID() == server.getServerId()) {
            server.setPeerState(PeerServer.ServerState.LEADING);
        } else if(server.getPeerState() == PeerServer.ServerState.LOOKING) {
            server.setPeerState(PeerServer.ServerState.FOLLOWING);
        }
        logger.fine("[" + Thread.currentThread().getName() + "]" + "Accepted Leader: " + n.getProposedLeaderID());
        //clear out the incoming queue before returning
        incomingMessages.clear();
        return n;
    }

    /*
     * We return true if one of the following two cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     */
    protected boolean supersedesCurrentVote(long newId, long newEpoch) {
        return (newEpoch > server.getPeerEpoch()) || ((newEpoch == server.getPeerEpoch()) && (newId > this.proposedLeader));
    }

    /**
     * Termination predicate. Given a set of votes, determines if we have sufficient support for the proposal to declare the end of the election round.
     * Who voted for who isn't relevant, we only care that each server has one current vote.
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification> votes, Vote proposal) {
        //is the number of votes for the proposal > the size of my peer server’s quorum?
        int count = 0;
        for (ElectionNotification n : votes.values()) {
            if (proposal.equals(n)) {
                count++;
            }
        }
        return count >= server.getQuorumSize();
    }
}