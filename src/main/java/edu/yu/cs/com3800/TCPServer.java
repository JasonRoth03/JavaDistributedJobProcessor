package edu.yu.cs.com3800;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class TCPServer implements Runnable, LoggingServer {
    private final Socket socket;
    private final LinkedBlockingQueue<RoundRobinMessage> roundRobinMessageQueue;
    private final Logger logger;
    private final int leaderPort;

    public TCPServer(int leaderPort, Socket socket, LinkedBlockingQueue<RoundRobinMessage> roundRobinMessageQueue) {
        this.leaderPort = leaderPort;
        this.socket = socket;
        this.roundRobinMessageQueue = roundRobinMessageQueue;

        try {
            this.logger = initializeLogging("TCPServer-toFollowerPort[" + socket.getPort() + "]");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void run() {
        LinkedBlockingQueue<Message> responseQueue = new LinkedBlockingQueue<>();
        try {
            logger.fine("[" + leaderPort + "] attempting to get socket input stream");
            InputStream in = socket.getInputStream();
            logger.fine("[" + leaderPort + "] Successfully acquired socket input stream");
            logger.fine("[" + leaderPort + "] attempting to get message from input stream");
            Message message = new Message(Util.readAllBytesFromNetwork(in));
            logger.fine("[" + leaderPort + "] got message from input stream, message: " + message);
            roundRobinMessageQueue.offer(new RoundRobinMessage(message, responseQueue));
            if(message.getMessageType() == Message.MessageType.COMPLETED_WORK){
                //Old request waiting to return response back to the leader
                //Don't wait for a response back to forward to gateway
                //1. Close socket
                socket.close();
                //2. end this thread
                return;
            }
            logger.fine("[" + leaderPort + "] gave message to round robin queue");
            Message response = responseQueue.take();
            logger.fine("[" + leaderPort + "] received response from worker: " + response);
            OutputStream out = socket.getOutputStream();
            out.write(response.getNetworkPayload());
            out.flush();
            socket.close();
        } catch (IOException | InterruptedException e) {
            if(!socket.isClosed()) {
                try {
                    socket.close();
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
            throw new RuntimeException(e);
        }
    }
}
