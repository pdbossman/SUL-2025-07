# goCQL Concurrency Simulator App

This application is intended to allow users to simulate concurrency scenarios with ScyllaDB.  To allow the user to drive high, or controlled concurrency scenarios, while changing other client and server configuration parameters, to observe how the application and ScyllaDB react.
The intent is to help users test different scenarios, and find appropriate client and server configuration settings.

Examples of configuration parameters include:
- Client timeout settings
- Server timeout settings
- Client connection and connection retry policies and settings
- Client speculative retry policy and settings
- Client request timeout retry policies and settings
- Server max_concurrent_requests_per_shard
- Different cluster key and data lengths, to observe effects of different payload sizes

## Purpose

Provide an easy to use application to run a variety of scenarios quickly, and observe effects on a server.
- Uncontrolled concurrency
- Controlling application concurrency via application side (semaphore)
- Protecting the server by lowering `max_concurrent_requests_per_shard`
- Simulate different client and server timeout settings


## Key Testing Scenarios

### Overload Simulation
- **Unbounded concurrency** - Overwhelming the server with excessive concurrent requests
- **Timeout misconfigurations** - Server timeout > client timeout scenarios that overload the server
- **Aggressive retry policies** - Overuse of client and/or server speculative retry mechanisms

### Performance Optimization
After demonstrating common overload scenarios and observing their impact in monitoring tools, learn to optimize performance by:
- Controlling concurrency levels appropriately
- Setting server timeout less than client timeout to avoid zombie work
- Adjusting client speculative retry and timeout policies with reasonable retry counts, minimum and maximum delays
- Tuning server-side timeouts
- Adjusting `max_concurrent_requests_per_shard` to familiarize with server side overload protection, understand how exponential backoff can be used to retry shed requests with delay

## Getting Started

### 1. Database Setup

Create the required keyspace and table in ScyllaDB:

```sql
CREATE KEYSPACE kstest1 WITH replication = { 
    'class' : 'NetworkTopologyStrategy',
    'AWS_US_EAST_1': '3'
} AND durable_writes = true AND tablets = {'enabled': false};

USE kstest1; 

CREATE TABLE tbtest1 (
    partitionkey1 text,
    clusterkey1 text,
    data1 text,
    data2 text,
    PRIMARY KEY(partitionkey1, clusterkey1)
);
```

**Note:** The application generates partition keys using random UUIDs. You can control the length of random cluster keys and data fields to adjust payload size.

### 2. Go Installation (Ubuntu)

```bash
# Download and install Go
wget -c https://go.dev/dl/go1.24.4.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.24.4.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin

# Configure PATH permanently
vim ~/.bashrc
```

Add the following to your `~/.bashrc`:

```bash
# Add Go to PATH if it exists
if [ -d "/usr/local/go/bin" ] ; then
    PATH="/usr/local/go/bin:$PATH"
fi

# Add GOPATH to PATH if it exists
if [ -d "$HOME/go/bin" ] ; then
    PATH="$HOME/go/bin:$PATH"
fi
```

```bash
# Apply changes and verify installation
source ~/.bashrc
go version
```

### 3. Application Setup

```bash
# Clone the repository (after setting up git keys)
git clone git@github.com:pdbossman/SUL-2025-07.git

# Build the application
cd ~/SUL-2025-07
mkdir ~/build
go mod tidy
go build -a -tags "gocql_debug" -o ../build/gorequest ./cmd/gorequest

# Set up environment variables
export PASSWORD=YOUR_PASSWORD_HERE
export UNAME=app1_user1

# Configure cluster connection
cp Example-ClusterConfig.json ClusterConfig.json
vim ClusterConfig.json  # Edit datacenter, contact points, and other settings
```

## Usage

### Command Line Options

```bash
../build/gorequest --help
```

| Flag | Description | Default |
|------|-------------|---------|
| `-clusterKey1Len` | Length of random data in clustering key | 25 |
| `-config` | Config file name | ClusterConfig.json |
| `-data1Len` | Length of random data in data1 field | 500 |
| `-password` | Database password | "" |
| `-progressInterval` | Number of records between status intervals | 10000 |
| `-qryIdempotent` | Write idempotent flag | 0 |
| `-readConcurrency` | Number of concurrent reads during read test | 100 |
| `-speculativeRetry` | Enable speculative retry (0=off, 1=on) | 0 |
| `-srNumAttempts` | Number of speculative retry attempts | - |
| `-srTimeoutDelay` | Milliseconds between speculative retry attempts | 60 |
| `-totalReads` | Total reads to perform | 1 |
| `-totalWrites` | Total writes to perform | 100000 |
| `-username` | Database username | app1_user1 |
| `-writeConcurrency` | Maximum concurrent transactions | 50 |

### Example Execution

```bash
nohup ../build/gorequest \
    -username $UNAME \
    -password $PASSWORD \
    -totalWrites 1000000 \
    -writeConcurrency 100 \
    -totalReads 100000 \
    -readConcurrency 100 \
    -speculativeRetry 0 \
    -clusterKey1Len 25 \
    -config ClusterConfig.json \
    -progressInterval 1000 \
    > test2.log 2>&1 &
```

This example runs the application in the background, writing results to `test2.log`, with 1 million writes at 100 concurrent transactions and 100,000 reads at 100 concurrent operations.

The code is compiled with debug to give greater visibility into driver state during execution.

cat test2.log | grep -i info


## Monitoring and Analysis

Monitor your ScyllaDB cluster during testing to observe the impact of different configurations and identify optimization opportunities. Pay attention to metrics like latency, throughput, error rates, and resource utilization to understand how various settings affect performance under stress conditions.