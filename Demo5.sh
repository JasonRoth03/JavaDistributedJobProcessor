#!/bin/bash
#Jason Roth and Tani Gross
#The HTTP endpoints for the gateway/gateway peer server is the gateway http port (here its 8888),  /logs/summary and /logs/verbose
#The Http Endpoints for each peer server is the udp port + 3, /logs/summary or /logs/verbose (peer ports go from 8010 - 8070)

# Ensure all output goes to both console and log file
exec 1> >(tee "output.log")
exec 2>&1

echo "Starting cluster test at $(date)"

# Constants
GATEWAY_HTTP_PORT=8888
GATEWAY_UDP_PORT=8000
CLUSTER_SIZE=8
NUM_OBSERVERS=1
HEARTBEAT_INTERVAL=3  # Assuming 10 seconds, adjust as needed
LEADER_ID=-1
FIRST_LEADER_ID=-1
CURRENT_LEADER_ID=-1

# Step 1: Build the project and run tests
#echo "Building project and running tests..."
mvn clean compil
mvn test

if [ $? -ne 0 ]; then
    echo "Maven build failed"
    exit 1
fi

# Array to store PIDs
declare -a SERVER_PIDS

# Step 2: Start Gateway
echo "Starting Gateway..."
java -cp target/classes edu.yu.cs.com3800.stage5.GatewayServer \
    $GATEWAY_HTTP_PORT $GATEWAY_UDP_PORT $CLUSTER_SIZE &
GATEWAY_PID=$!
#echo "Started Gateway with PID $GATEWAY_PID"
sleep 2

# Step 3: Start peer servers
echo "Starting peer servers..."
for i in $(seq 1 $((CLUSTER_SIZE-1)))
do
    PORT=$((8000 + i * 10))
    java -cp target/classes edu.yu.cs.com3800.stage5.PeerServerImpl \
        $PORT $i $NUM_OBSERVERS $CLUSTER_SIZE &
    SERVER_PIDS[$i]=$!
    #echo "Started server $i on port $PORT with PID ${SERVER_PIDS[$i]}"
done



# Function to check for leader
check_and_print() {
    CLUSTER_STATUS=$(curl -s "http://localhost:$GATEWAY_HTTP_PORT/cluster-info")
    if [[ $CLUSTER_STATUS == *"LEADER"* ]]; then
        CURRENT_LEADER_ID=$(echo "$CLUSTER_STATUS" | grep "LEADER" | grep -o "Server [0-9]" | cut -d' ' -f2)

        if [ $FIRST_LEADER_ID -eq -1 ]; then
            FIRST_LEADER_ID=$CURRENT_LEADER_ID
            echo "First leader elected: Server $FIRST_LEADER_ID"
        fi

        echo "Current cluster status:"
        echo "$CLUSTER_STATUS"
        echo "Current leader is Server $CURRENT_LEADER_ID"
        return 0
    else
        echo "No leader found. Status: "
        echo $CLUSTER_STATUS
        return 1
    fi
}

check_leader() {
    while true; do
        CLUSTER_STATUS=$(curl -s "http://localhost:$GATEWAY_HTTP_PORT/cluster-info")
        NEW_LEADER_ID=$(echo "$CLUSTER_STATUS" | grep "LEADER" | grep -o "Server [0-9]" | cut -d' ' -f2)

        if [ ! -z "$NEW_LEADER_ID" ] && [ "$NEW_LEADER_ID" != "$CURRENT_LEADER_ID" ]; then
            CURRENT_LEADER_ID=$NEW_LEADER_ID
            echo "New leader elected: Server $CURRENT_LEADER_ID"
            break
        fi
        echo "Waiting for new leader... Current status: $CLUSTER_STATUS"
        sleep 2
    done
}

sleep 8

# Wait for leader election
echo "Waiting for leader election..."
while ! check_and_print; do
    sleep 1
done

# Function to send request and get response
send_request() {
    local TEST_CLASS="package edu.yu.cs.fall2019.com3800.stage1;
    public class HelloWorld {
        public String run() {
            return \"Hello world! Request $1\";
        }
    }"

    echo "Sending request $1..."
    RESPONSE=$(curl -s -X POST -H "Content-Type: text/x-java-source" \
        -d "$TEST_CLASS" "http://localhost:$GATEWAY_HTTP_PORT/compileandrun")
    echo "Response for request $1: $RESPONSE"
}

FIRST_LEADER_PORT=$LEADER_PORT

# Send 9 initial requests
#echo "Sending 9 initial requests..."
for i in {1..9}; do
    send_request $i
done

# Kill a follower
#echo "Killing a follower..."
FOLLOWER_ID=$(curl -s "http://localhost:$GATEWAY_HTTP_PORT/cluster-info" | grep "FOLLOWER" | head -n1 | grep -o "Server [0-9]" | cut -d' ' -f2)
if [ ! -z "$FOLLOWER_ID" ]; then
    FOLLOWER_PID=${SERVER_PIDS[$FOLLOWER_ID]}
    echo "Killing follower (Server $FOLLOWER_ID) with PID $FOLLOWER_PID"
    kill -9 $FOLLOWER_PID 2>/dev/null
    unset SERVER_PIDS[$FOLLOWER_ID]
fi

# Wait heartbeat interval * 10
echo "Waiting for heartbeat interval * 15..."
sleep $((HEARTBEAT_INTERVAL * 15))

#print out the cluster
check_and_print

sleep 1

# Kill leader - modify this section
LEADER_ID=$(curl -s "http://localhost:$GATEWAY_HTTP_PORT/cluster-info" | grep "LEADER" | grep -o "Server [0-9]" | cut -d' ' -f2)
if [ ! -z "$LEADER_ID" ]; then
    LEADER_PID=${SERVER_PIDS[$LEADER_ID]}
    echo "Killing leader (Server $LEADER_ID) with PID $LEADER_PID"
    kill -9 $LEADER_PID 2>/dev/null
    unset SERVER_PIDS[$LEADER_ID]
fi
sleep 1
# array to hold request pids
REQUEST_PIDS=()

for i in {10..18}; do
  send_request "$i" &
  thePid="$!"
  #echo "Launched send_request for $i with PID=$thePid"
  REQUEST_PIDS+=("$!")  # "$!" is the PID of the last background job
done

# Wait *only* on the request processes
for pid in "${REQUEST_PIDS[@]}"; do
  wait "$pid"
done

echo "Received response for all background requests"
# Wait for new leader
echo "Waiting for new leader..."
check_leader

# Send one final request
send_request "final"

sleep 2

# List gossip log files
echo "\nGossip log files:"
find . -name "*Gossip*.txt" 2>/dev/null

# Function to cleanup processes
cleanup() {
    echo "Shutting down cluster..."

    # Kill the gateway process
    if [ -n "$GATEWAY_PID" ]; then
        #echo "Stopping Gateway with PID $GATEWAY_PID..."
        kill -9 $GATEWAY_PID 2>/dev/null
    fi

    # Kill each peer server process
    for pid in "${SERVER_PIDS[@]}"; do
        if [ -n "$pid" ]; then
            #echo "Stopping server with PID $pid..."
            kill -9 $pid 2>/dev/null
        fi
    done

    # Ensure no remaining processes
    #echo "Ensuring no stray processes remain..."
    pkill -f "edu.yu.cs.com3800.stage5" 2>/dev/null

    echo "Cluster shutdown completed."
    exit 0
}
trap cleanup SIGINT SIGTERM EXIT

# Clean shutdown after all tasks complete
cleanup