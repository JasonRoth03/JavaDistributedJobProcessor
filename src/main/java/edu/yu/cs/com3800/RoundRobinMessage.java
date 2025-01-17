package edu.yu.cs.com3800;

import java.util.concurrent.LinkedBlockingQueue;

public class RoundRobinMessage {
    private final Message request;
    private final LinkedBlockingQueue<Message> response;

        public RoundRobinMessage(Message request, LinkedBlockingQueue<Message> response) {
            this.request = request;
            this.response = response;
        }

        public Message getMessage() {
            return request;
        }

        public LinkedBlockingQueue<Message> getResponseQueue() {
            return response;
        }
}
