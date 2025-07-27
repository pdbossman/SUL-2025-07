package scylla

import (
	"time"
	"github.com/gocql/gocql"
	"encoding/json"
	"io/ioutil"
	"log"
	"strings"

	//hostpool "github.com/hailocab/go-hostpool"
)

type ClusterConfig struct {
	Consistency           string `json:"Consistency"`
	Keyspace              string `json:"Keyspace"`
	LocalDC               string `json:"LocalDC"`
	Rack                  string `json:"Rack"`
	Hosts                 string `json:"Hosts"`
	ConnectTimeout        int    `json:"ConnectTimeout"`
	ReconnectInterval     int    `json:"ReconnectInterval"`
	MaxRequestsPerConn    int    `json:"MaxRequestsPerConn"`
	Port                  int    `json:"Port"`
	Timeout               int    `json:"Timeout"`
	WriteTimeout          int    `json:"WriteTimeout"`
    UseReconnectionPolicy bool `json:"UseReconnectionPolicy"`
    UseRetryPolicy        bool `json:"UseRetryPolicy"`
	DisableInitialHostLookup bool `json:"DisableInitialHostLookup"`
    ConnectionRetryPolicy struct {
        PolicyName      string `json:"PolicyName"`
        MaxRetries      int `json:"MaxRetries"`
        InitialInterval int `json:"InitialInterval"`
        MaxInterval     int `json:"MaxInterval"`
    } `json:"ConnectionRetryPolicy"`
    RequestRetryPolicy struct {
        PolicyName     string `json:"PolicyName"`
        Min            int `json:"Min"`
        Max            int `json:"Max"`
        NumRetries     int `json:"NumRetries"`
    } `json:"RequestRetryPolicy"`
}


func CreateCluster(configFile string) *gocql.ClusterConfig {
	//https://pkg.go.dev/github.com/gocql/gocql#ClusterConfig
	//go retry policies:
	//https://github.com/apache/cassandra-gocql-driver/blob/253be521e1142cf4f0ef8fe30310c4e7675715f2/policies.go#L161
	// SimpleRetryPolicy, ExponentialBackoffRetryPolicy, DowngradingConsistencyRetryPolicy

    // Read the JSON configuration file
    data, err := ioutil.ReadFile(configFile)
    if err != nil {
        log.Fatal(err)
    }

    // Parse the JSON data into a Config struct
    var clusterConfig ClusterConfig
    err = json.Unmarshal(data, &clusterConfig)
    if err != nil {
        log.Fatal(err)
    }

	hosts := strings.Split(clusterConfig.Hosts, ",")

	connectionRetryPolicy := &gocql.ExponentialReconnectionPolicy{
		MaxRetries:      clusterConfig.ConnectionRetryPolicy.MaxRetries,
		InitialInterval: time.Duration(clusterConfig.ConnectionRetryPolicy.InitialInterval) * time.Millisecond, 
		MaxInterval:     time.Duration(clusterConfig.ConnectionRetryPolicy.MaxInterval) * time.Millisecond,
	}
    requestRetryPolicy := &gocql.ExponentialBackoffRetryPolicy{
		Min:	time.Duration(clusterConfig.RequestRetryPolicy.Min) * time.Millisecond,  
        Max:	time.Duration(clusterConfig.RequestRetryPolicy.Max) * time.Millisecond,
        NumRetries: clusterConfig.RequestRetryPolicy.NumRetries,
    }

	//set consistency from config file
	consistency := getConsistencyLevel(clusterConfig.Consistency)

	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = clusterConfig.Keyspace
	cluster.ConnectTimeout = time.Duration(clusterConfig.ConnectTimeout) * time.Millisecond
	if clusterConfig.UseReconnectionPolicy {
		cluster.ReconnectionPolicy = connectionRetryPolicy
	}
	cluster.DisableInitialHostLookup = clusterConfig.DisableInitialHostLookup
	cluster.Timeout = time.Duration(clusterConfig.Timeout) * time.Millisecond
	cluster.WriteTimeout = time.Duration(clusterConfig.WriteTimeout) * time.Millisecond
	cluster.MaxRequestsPerConn = clusterConfig.MaxRequestsPerConn
	cluster.ReconnectInterval = time.Duration(clusterConfig.ReconnectInterval) * time.Millisecond

	if clusterConfig.UseRetryPolicy {
		cluster.RetryPolicy = requestRetryPolicy
	}
	cluster.Consistency = consistency

	//Host selection policies:
	//https://github.com/apache/cassandra-gocql-driver/blob/253be521e1142cf4f0ef8fe30310c4e7675715f2/policies.go#L299
	// roundRobinHostPolicy, DCAwareHostPolicy, TokenAwareHostPolicy, NonLocalReplicasFallback
	// HostPoolHostPolicy, DCAwareRoundRobinPolicy, RackAwareRoundRobinPolicy
	// use HostPoolHostPolicy which is supposed to avoid unresponsive nodes
	//cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.HostPoolHostPolicy(hostpool.NewEpsilonGreedy(nil, 0, &hostpool.LinearEpsilonValueCalculator{}),))
	// Enable token aware host selection policy, if using multi-dc cluster set a local DC.
	fallback := gocql.RoundRobinHostPolicy()
	rackAware := false
	if clusterConfig.LocalDC != "" && clusterConfig.Rack != "" {
		fallback = gocql.RackAwareRoundRobinPolicy(clusterConfig.LocalDC,clusterConfig.Rack)
		rackAware = true
	} else if clusterConfig.LocalDC != "" {
		fallback = gocql.DCAwareRoundRobinPolicy(clusterConfig.LocalDC)
	}

	// Only shuffle replicas if we're not rack aware.
	// Rack aware prefers a replica in the rack specified
	if rackAware {
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)
	} else {
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback,gocql.ShuffleReplicas())
	}
	return cluster
}

func getConsistencyLevel(consistencyValue string) gocql.Consistency {
	switch consistencyValue {
	case "any":
		return gocql.Any
	case "one":
		return gocql.One
	case "two":
		return gocql.Two
	case "three":
		return gocql.Three
	case "quorum":
		return gocql.Quorum
	case "all":
		return gocql.All
	case "localquorum":
		return gocql.LocalQuorum
	case "eachquorum":
		return gocql.EachQuorum
	case "localone":
		return gocql.LocalOne
	default:
		return gocql.LocalQuorum
	}
}