package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.PeerServer;
import edu.yu.cs.com3800.Vote;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@TestInstance(Lifecycle.PER_CLASS)
public class Stage5Test{
    private final int[] ports = {4000, 4010, 4020, 4030, 4040, 4050, 4060, 4070};
    private final int[] ports2 = {5000, 5010, 5020, 5030, 5040, 5050, 5060, 5070};
    private final int[] ports3 ={6000, 6010, 6020, 6030, 6040, 6050, 6060, 6070};
    private final int[] ports4 = {7000, 7010, 7020, 7030, 7040, 7050, 7060, 7070};
    private final int[] ports5 = {9000, 9010, 9020, 9030, 9040, 9050, 9060, 9070};
    private final int[][] allPorts = {ports, ports2, ports3, ports4, ports5};
    private ArrayList<PeerServerImpl> servers;
    private GatewayServer gateway;

//    @BeforeEach
//    public void setup() throws InterruptedException {
//        TimeUnit.SECONDS.sleep(10);
//
//    }
//
//    @AfterEach
//    public void teardown() throws InterruptedException {
//        TimeUnit.SECONDS.sleep(10);
//    }

    private String getUniqueJavaCode(int counter) {
        return String.format(
                "package edu.yu.cs.fall2019.com3800.stage1;\n\n" +
                        "public class HelloWorld%d\n{\n" +
                        "    public String run()\n    {\n" +
                        "        return \"Hello world from request %d!\";\n" +
                        "    }\n}\n",
                counter, counter
        );
    }

    /*@Test
    public void GossipTest() throws Exception {
        String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
        //step 1: create servers
        createServers();
        //step2.1: wait for servers to get started
        try {
            Thread.sleep(8000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        printLeaders();

        TimeUnit.SECONDS.sleep(90);

        Response r = sendClientRequest("localhost", 8888, validClass);

        //step 5: stop servers
        gateway.shutdown();
        stopServers();
    }*/

    //Send some work and then kill the leader right after and wait for response
    @Test
    void leaderFailureDuringWorkTest() throws Exception{
        String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
        createServers(1);

        //Wait for server spin up
        try{
            Thread.sleep(8000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        printLeaders();

        //Let servers gossip to discover each other
        TimeUnit.SECONDS.sleep(20);

        CompletableFuture<Response> responseFuture = new CompletableFuture<>();

        Thread clientThread = new Thread(() -> {
            Response response = sendClientRequest("localhost", 8888, validClass);
            responseFuture.complete(response);
        });
        clientThread.start();

        //kill leader
        servers.get(servers.size() - 1).shutdown();
        servers.remove(servers.size() - 1);

        Response r = responseFuture.get();

        //step 5: stop servers
        gateway.shutdown();
        stopServers();

    }

    //Leader fails before any work being sent, sleep to allow gossips, then send the work
    @Test
    void LeaderFail_no_prior_work() throws Exception {
        String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
        createServers(2);
        //Wait for server spin up
        try{
            Thread.sleep(6000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        printLeaders();

        //Let servers gossip to discover each other
        TimeUnit.SECONDS.sleep(20);

        //Kill Server 8 -> Leader
        servers.get(servers.size() - 1).shutdown();
        servers.remove(servers.size() - 1);

        //Wait for servers to discover leader failure
        TimeUnit.SECONDS.sleep(60);

        Response r = sendClientRequest("localhost", 8888, validClass);

        //step 5: stop servers
        gateway.shutdown();
        stopServers();

    }

    //Kill worker 1 (where the first work would have went), Leader should be able to see failed node and send work to next follower
    @Test
    void Worker1Fail_and_reassign_work() throws Exception {
        String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
        createServers(3);
        //Wait for server spin up
        try{
            Thread.sleep(6000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        printLeaders();

        //Let servers gossip to discover each other
        TimeUnit.SECONDS.sleep(20);

        //Kill Server 2 -> would have been first server assigned work
        servers.get(0).shutdown();
        servers.remove(0);

        //Wait for servers to discover server 2 failure
        TimeUnit.SECONDS.sleep(60);

        //request should go to 2
        Response r = sendClientRequest("localhost", 8888, validClass);

        //step 5: stop servers
        gateway.shutdown();
        stopServers();

    }

    //Basic test, start servers and send work, receive response
    @Test
    public void basicTest() throws Exception {
        String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
        //step 1: create servers
        createServers(4);
        //step2.1: wait for servers to get started
        try {
            Thread.sleep(8000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        printLeaders();

        Response r = sendClientRequest("localhost", 8888, validClass);
        //step 5: stop servers
        gateway.shutdown();
        stopServers();

    }

    private Response sendClientRequest(String hostName, int port, String src) {
        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        Response r = null;

        HttpRequest request = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(src))
                .uri(URI.create("http://" + hostName + ":" + port + "/compileandrun"))
                .setHeader("User-Agent", "httpClient")
                .header("Content-Type", "text/x-java-source")
                .build();
        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            String cached = response.headers().allValues("Cached-Response").get(0);
            r = new Response(response.statusCode(), response.body(), Boolean.parseBoolean(cached));
        }catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
        return r;
    }

    private void stopServers() {
        for (PeerServer server : servers) {
            server.shutdown();
        }
    }

    private void printLeaders() {
        for (PeerServer server : this.servers) {
            Vote leader = server.getCurrentLeader();
            if (leader != null) {
                System.out.println("Server on port " + server.getAddress().getPort() + " whose ID is " + server.getServerId() + " has the following ID as its leader: " + leader.getProposedLeaderID() + " and its state is " + server.getPeerState().name());
            }else{
                System.out.println("Server on port " + server.getAddress().getPort() + " whose ID is " + server.getServerId() + " has no leader  yet and its state is " + server.getPeerState().name());
            }
        }
    }

    private Map<Long, InetSocketAddress> createServers(int portNumber) throws IOException {
        //create IDs and addresses
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(8);
        for (int i = 0; i < this.ports.length; i++) {
            peerIDtoAddress.put(Integer.valueOf(i).longValue(), new InetSocketAddress("localhost", this.allPorts[portNumber][i]));
        }
        //create gateway server
        ConcurrentHashMap<Long, InetSocketAddress> cMap = new ConcurrentHashMap<>(peerIDtoAddress);
        cMap.remove(0L);
        gateway = new GatewayServer(8888, this.allPorts[portNumber][0], 0, 0L, cMap, 1);

        //create servers
        this.servers = new ArrayList<>(3);
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            if(entry.getKey() == 0L) continue;
            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
            map.remove(entry.getKey());
            PeerServerImpl server = new PeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map, 0L, 1);
            this.servers.add(server);
        }
        gateway.start();
        for(PeerServerImpl server : this.servers) {
            server.start();
        }

        return peerIDtoAddress;
    }

    static class Response{
        private final int code;
        private final String body;
        private final boolean cached;

        public Response(int code, String body, boolean cached) {
            this.code = code;
            this.body = body;
            this.cached = cached;
        }

        public Response(int code, String body) {
            this(code, body, false);
        }

        public int getCode() {
            return code;
        }

        public String getBody() {
            return body;
        }

        public boolean getCached() {
            return cached;
        }
    }

    static class ControlServer extends Thread {
        private final List<PeerServerImpl> servers;
        private ServerSocket serverSocket;
        private volatile boolean running = true;

        public ControlServer(List<PeerServerImpl> servers, int port) throws IOException {
            this.servers = servers;
            this.serverSocket = new ServerSocket(port);
        }

        @Override
        public void run() {
            while (running) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

                    String command = in.readLine();
                    try {
                        int serverId = Integer.parseInt(command);
                        for (PeerServerImpl server : servers) {
                            if (server.getServerId() == serverId) {
                                System.out.println("Killing server " + serverId);
                                server.shutdown();
                                servers.remove(server);
                                out.println("Server " + serverId + " killed");
                                break;
                            }
                        }
                    } catch (NumberFormatException e) {
                        out.println("Invalid server ID");
                    }

                    clientSocket.close();
                } catch (IOException e) {
                    if (running) {
                        e.printStackTrace();
                    }
                }
            }
        }

        public void shutdown() {
            running = false;
            try {
                serverSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}