# JavaDistributedJobProcessor
# Java Distributed Job Processor

A distributed systems project implementing a fault-tolerant Java code execution cluster, developed as part of Introduction to Distributed Systems coursework.

## Project Overview

This project demonstrates the implementation of core distributed systems concepts through a practical application that compiles and executes Java code across a cluster of nodes. The system was developed across 5 stages, each introducing additional distributed computing concepts and challenges.

## Core Features

### Distributed Processing
- HTTP gateway accepting Java code compilation/execution requests
- Cluster of worker nodes for distributed processing
- Round-robin work distribution
- Request result caching

### Fault Tolerance
- ZooKeeper-style leader election
- Gossip protocol for failure detection
- Automatic recovery from node failures
- Work redistribution on node failure

### System Architecture

The system consists of several key components:

1. **Gateway Server**
   - Entry point for client requests
   - HTTP interface for code submission
   - Response caching
   - Cluster state observation

2. **Peer Servers**
   - Three possible states: LOOKING, FOLLOWING, LEADING
   - Leader election participation
   - Java code processing
   - Health monitoring via gossip protocol

3. **JavaRunner**
   - Code compilation and execution
   - Isolated execution environment
   - Result capturing and return

### Network Communication

- UDP for cluster management and gossip protocol
- TCP for work distribution
- HTTP for client interactions

## Implementation Details

### Leader Election
- Based on ZooKeeper's Fast Leader Election algorithm
- Ensures single leader for work distribution
- Handles leader failures and re-election
- Epoch-based consistency

### Fault Tolerance Implementation
- Gossip-based failure detection
- Configurable timeouts
- Automatic failure recovery
- Work redistribution logic

### Work Distribution System
- Round-robin distribution among followers
- Response caching
- TCP-based reliable messaging
- Error handling and retry logic

### Monitoring and Logging
- Dual logging levels (summary and verbose)
- HTTP-accessible logs
- Cluster state tracking
- Performance monitoring

## Technical Components

### API Endpoints

1. `/compileandrun`
   - Accepts Java source code
   - Returns compilation/execution results

2. `/cluster-info`
   - Provides cluster state information
   - Shows node roles and status

3. `/logs/summary` and `/logs/verbose`
   - Access to system logs
   - Different detail levels

### Gossip Protocol Details
- Heartbeat-based detection
- Configurable intervals
- Failed node cleanup
- State synchronization

### Caching System
- In-memory result storage
- Hash-based key generation
- Leader change invalidation

## Learning Outcomes

This project demonstrates understanding of key distributed systems concepts:

- Distributed consensus through leader election
- Fault tolerance and high availability
- Distributed task processing
- Network communication protocols
- System monitoring and logging
- Cache coherence and invalidation

## Technologies Used

- Java
- TCP/UDP Networking
- HTTP Servers
- Gossip Protocols
- Leader Election Algorithms
- Distributed Caching
