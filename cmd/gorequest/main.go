package main

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Sample goCQL application to test application resiliency with ScyllaDB.
// Facilitate testing with different server and client settings
// Facilitate effects of concurrency controls and lack thereof
//
// Server settings:
//   - server timeouts (read, write, request, CAS, range, etc)
//   - max requests per shard
//
// Client settings
//   - Connection timeout settings
//   - Connection retry policy and settings (no retry, basic retry, exponential backoff with different delay/retries)
//   - request timeout
//   - request timeout retry policy and settings (None, SimpleRetry, ExponentialBackoffRetryPolicy, DowngradingConsistencyRetryPolicy)
//   - request speculative retry policy and settings (none, retries, delay)
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

import (
	"context"
	"fmt"
	"gorequest/internal/log"
	"gorequest/internal/scylla"
	"math/rand"
	"flag"
	"runtime"
	"strings"
	"sync"
	"sort"
	"time"
	"sync/atomic"
	"io/ioutil"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type TestRecord struct {
	PartitionKey1       string
	ClusterKey1        	string
	Data1         		string
	Data2 				string
}

type KeyPair struct {
    PartitionKey string
    ClusterKey   string
}

type WriteTestResults struct {
    totalWrites  int
    successWrites int
    failedWrites  int
    minLatency   time.Duration
    maxLatency   time.Duration
    avgLatency   time.Duration
    p95Latency   time.Duration
    p99Latency   time.Duration
    latencies    []time.Duration
}

type ReadTestResults struct {
    totalReads   int
    successReads int
    failedReads  int
    minLatency   time.Duration
    maxLatency   time.Duration
    avgLatency   time.Duration
    p95Latency   time.Duration
	p99Latency   time.Duration
    latencies    []time.Duration
}

func getRandomAlphanumericStringWithRand(stringlen int, localRand *rand.Rand) string {
    var dataBuilder strings.Builder
    dataBuilder.Grow(stringlen) // Pre-allocate capacity for better performance
    
    for i := 0; i < stringlen; i++ {
        dataBuilder.WriteRune(getRandomAlphanumericCharWithRand(localRand))
    }
    return dataBuilder.String()
}

func getRandomAlphanumericCharWithRand(localRand *rand.Rand) rune {
    // Generate a random number between 0 and 35
    randNum := localRand.Intn(36)

    // If the random number is less than 26, generate a letter
    if randNum < 26 {
        return rune('a' + randNum)
    } else {
        // Otherwise, generate a number
        return rune('0' + randNum - 26)
    }
}

func preGenerateTestData(totalRecords int, clusterKey1Len int, data1Len int, logger *zap.Logger) []TestRecord {
    startTime := time.Now()
    logger.Info(fmt.Sprintf("Pre-generating %d test records in parallel...", totalRecords))
    
    records := make([]TestRecord, totalRecords)
    
    // Determine number of workers (use CPU count or a reasonable default)
    numWorkers := runtime.NumCPU()
    if numWorkers > totalRecords {
        numWorkers = totalRecords
    }
    
    logger.Info(fmt.Sprintf("Using %d workers for data generation", numWorkers))
    
    var wg sync.WaitGroup
    
    // Calculate work per worker
    recordsPerWorker := totalRecords / numWorkers
    remainder := totalRecords % numWorkers
    
    // Launch workers
    for workerID := 0; workerID < numWorkers; workerID++ {
        wg.Add(1)
        
        // Calculate range for this worker
        startIdx := workerID * recordsPerWorker
        endIdx := startIdx + recordsPerWorker
        
        // Last worker handles remainder
        if workerID == numWorkers-1 {
            endIdx += remainder
        }
        
        go func(workerID, start, end int) {
            defer wg.Done()
            
            // Create a local random source for this worker to avoid contention
            localRand := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
            
            for i := start; i < end; i++ {
                newUUID, err := uuid.NewUUID()
                if err != nil {
                    panic(err)
                }
                
                records[i] = TestRecord{
                    PartitionKey1: newUUID.String(),
                    ClusterKey1:   getRandomAlphanumericStringWithRand(clusterKey1Len, localRand),
                    Data1:         getRandomAlphanumericStringWithRand(data1Len, localRand),
                    Data2:         getRandomAlphanumericStringWithRand(data1Len, localRand),
                }
                
                // Optional: Log progress per worker (less frequent to avoid log spam)
                if (i-start+1)%5000 == 0 {
                    logger.Info(fmt.Sprintf("Datagen progress - Worker %d: Generated %d/%d records", workerID, i-start+1, end-start))
                }
            }
            
            logger.Info(fmt.Sprintf("Datagen - Worker %d completed: generated %d records (indices %d-%d)", workerID, end-start, start, end-1))
        }(workerID, startIdx, endIdx)
    }
    
    // Wait for all workers to complete
    wg.Wait()

    duration := time.Since(startTime)
    recordsPerSecond := float64(totalRecords) / duration.Seconds()
    
    logger.Info(fmt.Sprintf("Datagen - pre-generation complete: %d records ready in %v (%.2f records/sec)", 
        totalRecords, duration, recordsPerSecond))
    return records
}

func generateRandomData(clusterKey1Len int, data1Len int, logger *zap.Logger) TestRecord {
	var testrecord TestRecord
	//logger.Info("006- right before uuid creation")
	newUUID, err := uuid.NewUUID()
	if err != nil {
			panic(err)
	}
	testrecord.PartitionKey1 = newUUID.String()
	//logger.Info("007 - before call to get alphanumeric string")
	testrecord.ClusterKey1 = getRandomAlphanumericString(clusterKey1Len)
	//logger.Info("008 - after call to get alphanumeric string - cluster key")
	testrecord.Data1 = getRandomAlphanumericString(data1Len)
	//logger.Info("009 - after call to get alphanumeric string - dataLen1")

	return testrecord
}

func getRandomAlphanumericString(stringlen int) string {
	var data1Builder strings.Builder
	for i := 0; i<stringlen; i++ {
		data1Builder.WriteRune(getRandomAlphanumericChar())
	}
	return data1Builder.String()
}

func getRandomAlphanumericChar() rune {
    // Generate a random number between 0 and 35
    randNum := rand.Intn(36)

    // If the random number is less than 26, generate a letter
    if randNum < 26 {
        return rune('a' + randNum)
    } else {
        // Otherwise, generate a number
        return rune('0' + randNum - 26)
    }
}

func insertQuery(session *gocql.Session, ctx context.Context, partitionkey1 string, clusterkey1 string, data1 string, data2 string, speculativeRetry int, srNumAttempts int, srTimeoutDelay int, qryIdempotent bool, logger *zap.Logger) bool {
	failed := false
	//sp := &gocql.SimpleSpeculativeExecution{NumAttempts: 2, TimeoutDelay: 1 * time.Millisecond}

	query := session.Query("INSERT INTO tbtest1 (partitionkey1,clusterkey1,data1,data2) VALUES (?,?,?,?)", partitionkey1, clusterkey1, data1, data2)
	//query.SetSpeculativeExecutionPolicy(sp)
	//if speculativeRetry != 0 {
	//	sp := &gocql.SimpleSpeculativeExecution{NumAttempts: srNumAttempts, TimeoutDelay: time.Duration(srTimeoutDelay) * time.Millisecond}
	//	query.SetSpeculativeExecutionPolicy(sp)
	//}
	query.Idempotent(qryIdempotent)

	if err := query.Exec(); err != nil {
		logger.Error("insert tbtest1 "+partitionkey1+" "+clusterkey1, zap.Error(err))
		failed = true
	}
	return failed
}

func calculateWriteLatencies(writeLatencies []time.Duration, successCount int64, totalLatency time.Duration, minLatency time.Duration, maxLatency time.Duration) WriteTestResults {
    // Calculate average latency
    var avgLatency time.Duration
    if successCount > 0 {
        avgLatency = totalLatency / time.Duration(successCount)
    }

    // Calculate percentile latencies
    var p95Latency time.Duration
    var p99Latency time.Duration

    if len(writeLatencies) > 0 {
        // Sort latencies for percentile calculations
        sort.Slice(writeLatencies, func(i, j int) bool {
            return writeLatencies[i] < writeLatencies[j]
        })

        // Calculate p95 latency
        idx95 := int(float64(len(writeLatencies)) * 0.95)
        if idx95 < len(writeLatencies) {
            p95Latency = writeLatencies[idx95]
        } else {
            p95Latency = writeLatencies[len(writeLatencies)-1]
        }

        // Calculate p99 latency
        idx99 := int(float64(len(writeLatencies)) * 0.99)
        if idx99 < len(writeLatencies) {
            p99Latency = writeLatencies[idx99]
        } else {
            p99Latency = writeLatencies[len(writeLatencies)-1]
        }
    }

    return WriteTestResults{
        totalWrites:   int(successCount),
        successWrites: int(successCount),
        failedWrites:  0, // We only track latencies for successful writes
        minLatency:    minLatency,
        maxLatency:    maxLatency,
        avgLatency:    avgLatency,
        p95Latency:    p95Latency,
        p99Latency:    p99Latency,
        latencies:     writeLatencies,
    }
}

func SelectQuery(session *gocql.Session, loop int64, logger *zap.Logger) {
	logger.Info("Displaying Results:")
	qIP := session.Query("SELECT cast(listen_address as varchar) as listen_address FROM system.local")
	var listen_address string
	itIP := qIP.Iter()
	for itIP.Scan(&listen_address) {
		logger.Info("\t" + listen_address + " " )
	}
	if err := itIP.Close(); err != nil {
		logger.Warn("select system.local", zap.Error(err))
	}

	q := session.Query("SELECT partitionkey1,clusterkey1,data1,data2 FROM tbtest1 limit 1")
	var partitionkey1,clusterkey1,data1,data2 string
	it := q.Iter()
	found := false
	for it.Scan(&partitionkey1, &clusterkey1, &data1, &data2) {
		logger.Info("\t" + partitionkey1 + " " + clusterkey1)
		found = true
	}
	if err := it.Close(); err != nil {
		logger.Warn("select kstest1.tbtest1", zap.Error(err))
	}

	if found {
		q := session.Query("SELECT partitionkey1,clusterkey1,data1,data2 FROM tbtest1 WHERE partitionkey1 = ? AND clusterkey1 = ?", &partitionkey1, &clusterkey1)
		var i int64
		for i = 0; i < loop; i++ {
			var partitionkey1,clusterkey1,data1,data2 string
			it := q.Iter()
			for it.Scan(&partitionkey1, &clusterkey1, &data1, &data2) {
				logger.Info("\t" + partitionkey1 + " " + clusterkey1)
			}
			if err := it.Close(); err != nil {
				logger.Warn("select tbtest1", zap.Error(err))
			}
		}
	}
}

func runReadTest(session *gocql.Session, testRecords []TestRecord, totalReads int64, concurrency int, 
        speculativeRetry int, srNumAttempts int, srTimeoutDelay int, qryIdempotent bool, progressInterval int64, 
        logger *zap.Logger) ReadTestResults {
    if len(testRecords) == 0 {
        logger.Warn("No test records available for read testing, skipping read test")
        return ReadTestResults{}
    }

    // Create a semaphore to limit concurrency
    sem := make(chan struct{}, concurrency)

    // Create a wait group to track completion
    var wg sync.WaitGroup
    wg.Add(int(totalReads))

    // Use atomic counters for tracking progress and stats
    var successCnt int64 = 0
    var errorCnt int64 = 0
    var readsAttempted int64 = 0

    // Create a slice to collect latency measurements
    latencies := make([]time.Duration, 0, totalReads)
    var latencyMutex sync.Mutex  // Mutex to protect the latencies slice

    // Min and max latency
    var minLatency time.Duration
    var maxLatency time.Duration
    var minLatencyMutex sync.Mutex // Mutex to protect minLatency
    var maxLatencyMutex sync.Mutex // Mutex to protect maxLatency

    // Total latency for average calculation
    var totalLatency time.Duration
    var totalLatencyMutex sync.Mutex // Mutex to protect totalLatency

    logger.Info(fmt.Sprintf("Starting %d read operations with concurrency %d using %d available records", 
        totalReads, concurrency, len(testRecords)))

    // Launch all read operations
	startTime := time.Now()
    for i := int64(0); i < totalReads; i++ {
        // Acquire semaphore slot before read
        sem <- struct{}{}

        atomic.AddInt64(&readsAttempted, 1)

        // Log progress periodically
        currentReadsAttempted := atomic.LoadInt64(&readsAttempted)
        if currentReadsAttempted%progressInterval == 0 || currentReadsAttempted == totalReads {
        //if currentReadsAttempted%int64(concurrency*10) == 0 || currentReadsAttempted == totalReads {
            currentSuccessCnt := atomic.LoadInt64(&successCnt)
            currentErrorCnt := atomic.LoadInt64(&errorCnt)
            logger.Info(fmt.Sprintf("Read progress: %d/%d (%.1f%%), Success: %d, Failures: %d , Start Time: %s", 
                currentReadsAttempted, totalReads, 
                float64(currentReadsAttempted)/float64(totalReads)*100,
                currentSuccessCnt, currentErrorCnt, startTime.Format(time.RFC3339Nano)))
        }

        go func() {
            defer wg.Done()
            defer func() {
                <-sem // Release semaphore slot after read
            }()
            
            // Randomly select a record from the pre-generated test data
            recordIndex := rand.Intn(len(testRecords))
            testRecord := testRecords[recordIndex]
            
            // Prepare the query using the randomly selected test record
            q := session.Query("SELECT partitionkey1, clusterkey1, data1, data2 FROM tbtest1 WHERE partitionkey1 = ? AND clusterkey1 = ?",
                testRecord.PartitionKey1, testRecord.ClusterKey1)
            
            // Apply speculative retry if enabled
            if speculativeRetry != 0 {
                sp := &gocql.SimpleSpeculativeExecution{
                    NumAttempts: srNumAttempts, 
                    TimeoutDelay: time.Duration(srTimeoutDelay) * time.Millisecond,
                }
                q.SetSpeculativeExecutionPolicy(sp)
            }
            
            // Set idempotence flag
            q.Idempotent(qryIdempotent)
            
            // Execute the query
            var partitionkey1, clusterkey1, data1, data2 string
            qryStartTime := time.Now()
            if err := q.Scan(&partitionkey1, &clusterkey1, &data1, &data2); err != nil {
                logger.Error("Read query failed",
                    zap.String("partitionkey1", testRecord.PartitionKey1),
                    zap.String("clusterkey1", testRecord.ClusterKey1),
                    zap.Error(err))
                atomic.AddInt64(&errorCnt, 1)
            } else {
                // Calculate and record latency
                duration := time.Since(qryStartTime)
                
                // Update min/max latency
                minLatencyMutex.Lock()
                if minLatency == 0 || duration < minLatency {
                    minLatency = duration
                }
                minLatencyMutex.Unlock()
                
                maxLatencyMutex.Lock()
                if duration > maxLatency {
                    maxLatency = duration
                }
                maxLatencyMutex.Unlock()
                
                // Update total latency
                totalLatencyMutex.Lock()
                totalLatency += duration
                totalLatencyMutex.Unlock()
                
                // Add to latencies slice
                latencyMutex.Lock()
                latencies = append(latencies, duration)
                latencyMutex.Unlock()
                
                atomic.AddInt64(&successCnt, 1)
            }
        }()
    }

    // Wait for all reads to complete
    wg.Wait()

    // Final read count
    finalSuccessCnt := atomic.LoadInt64(&successCnt)
    finalErrorCnt := atomic.LoadInt64(&errorCnt)

    // Log final stats
    logger.Info(fmt.Sprintf("Read test completed. Success: %d, Failures: %d", 
        finalSuccessCnt, finalErrorCnt))

    // Calculate average latency
    var avgLatency time.Duration
    if finalSuccessCnt > 0 {
        avgLatency = totalLatency / time.Duration(finalSuccessCnt)
    }

    // Calculate percentile latencies
    var p95Latency time.Duration
	var p99Latency time.Duration

    if len(latencies) > 0 {
        // Sort latencies for percentile calculations
        sort.Slice(latencies, func(i, j int) bool {
            return latencies[i] < latencies[j]
        })

        // Calculate p95 latency
        idx95 := int(float64(len(latencies)) * 0.95)
        if idx95 < len(latencies) {
            p95Latency = latencies[idx95]
        } else {
            p95Latency = latencies[len(latencies)-1]
        }
    }

    // Calculate p99 latency
    idx99 := int(float64(len(latencies)) * 0.99)
    if idx99 < len(latencies) {
        p99Latency = latencies[idx99]
    } else {
        p99Latency = latencies[len(latencies)-1]
    }

    // Prepare and return results
    results := ReadTestResults{
        totalReads:   int(totalReads),
        successReads: int(finalSuccessCnt),
        failedReads:  int(finalErrorCnt),
        minLatency:   minLatency,
        maxLatency:   maxLatency,
        avgLatency:   avgLatency,
        p95Latency:   p95Latency,
		p99Latency:   p99Latency,
        latencies:    latencies,
    }

    return results
}

func getSystemConfig(session *gocql.Session, logger *zap.Logger) {   
    query := session.Query("SELECT name, source, type, value FROM system.config where name in ('write_request_timeout_in_ms', 'request_timeout_in_ms', 'truncate_request_timeout_in_ms', 'range_request_timeout_in_ms', 'read_request_timeout_in_ms', 'cas_contention_timeout_in_ms', 'counter_write_request_timeout_in_ms', 'max_concurrent_requests_per_shard')")
    iter := query.Iter()
    
    var name, source, datatype, value string
    configCount := 0
    
    logger.Info("ScyllaDB System Configuration:")
    logger.Info("=" + strings.Repeat("=", 80))
    
    for iter.Scan(&name, &source, &datatype, &value) {
        configCount++
        logger.Info(fmt.Sprintf("%s = %s datatype: %s source: %s ", name, value, datatype, source))
    }
    
    if err := iter.Close(); err != nil {
        logger.Error("Error reading system.config", zap.Error(err))
    } else {
        logger.Info(fmt.Sprintf("Retrieved %d system configuration parameters", configCount))
    }
    
    logger.Info("=" + strings.Repeat("=", 80))
}

func main() {
    // Define flags
	username := flag.String("username", "app1_user1", "database username")
	password := flag.String("password", " ", "database password")
	configFile := flag.String("config", " ClusterConfig.json", "config file name")
    writeConcurrency := flag.Int("writeConcurrency", 50, "Maximum concurrent transactions.  Default 50.")
	totalWrites := flag.Int("totalWrites", 100000, "Total writes to perform.  Default 100000")
	speculativeRetry := flag.Int("speculativeRetry", 0, "0 for off, 1 for on.  Default is 0.")
	srNumAttempts := flag.Int("srNumAttempts", 0,"Number of speculative retry attempts")
	srTimeoutDelay := flag.Int("srTimeoutDelay", 60,"Number of milliseconds between speculative retry attempts")
	qryIdempotent := flag.Int("qryIdempotent", 0,"Write idempotent flag")
	clusterKey1Len := flag.Int("clusterKey1Len", 25,"Length of random data in clustering key")
	data1Len := flag.Int("data1Len", 500,"Length of random data in data1 field")
	progressInterval := flag.Int64("progressInterval", 10000,"Number of records between status interval")
	totalReads := flag.Int64("totalReads", 1, "Total reads to perform.  Default 100000")
	readConcurrency := flag.Int("readConcurrency", 100, "Number of concurrent reads to perform during read test")
    flag.Parse()

    // Add random seed initialization
    rand.Seed(time.Now().UnixNano())

	var wg sync.WaitGroup
	wg.Add(*totalWrites)

	logger := log.CreateLogger("debug")
	qryIdempotentBool := false
	if *qryIdempotent != 0 {
		qryIdempotentBool = true
	}

    sem := make(chan struct{}, *writeConcurrency) // Define semaphore with writeConcurrency limit

	
	cluster := scylla.CreateCluster(*configFile)
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: *username,
		Password: *password}

	session, err := gocql.NewSession(*cluster)
	if err != nil {
		logger.Fatal("unable to connect to scylla", zap.Error(err))
	}
	defer session.Close()
	ctx := context.Background()

    // Read the JSON configuration file
    data, err := ioutil.ReadFile(*configFile)
    if err != nil {
        logger.Fatal("Unable to read config file", zap.Error(err))
    }	

	goRoutines := runtime.GOMAXPROCS(runtime.NumCPU())

	var errorCnt int64 = 0
	var successCnt int64 = 0
	var rowAttempt int64 = 0
	var currentErrorCnt int64 = 0
	var currentSuccessCnt int64 = 0
	var currentRowAttempt int64 = 0
	// Add these for write latency tracking
	writeLatencies := make([]time.Duration, 0, *totalWrites)
	var writeLatencyMutex sync.Mutex
	var writeMinLatency time.Duration
	var writeMaxLatency time.Duration
	var writeMinLatencyMutex sync.Mutex
	var writeMaxLatencyMutex sync.Mutex
	var writeTotalLatency time.Duration
	var writeTotalLatencyMutex sync.Mutex

    // PRE-GENERATE ALL TEST DATA
    testRecords := preGenerateTestData(*totalWrites, *clusterKey1Len, *data1Len, logger)
    logger.Info("All test data pre-generated, starting insert phase...")

	logger.Info("001-Start of for loop")
	startTime := time.Now()
	writeJobStartTime := time.Now()

	for i := 0; i < *totalWrites; i++ {
		//logger.Info("002-Inside for loop")

		// Acquire semaphore slot before insert
		sem <- struct{}{}

		go func(recordIndex int) {
			defer wg.Done()
			defer func() {
				<-sem // Release semaphore slot after insert
			}()

            // Use pre-generated data instead of generating on-the-fly
            testrecord := testRecords[recordIndex]
			
			//testrecord := generateRandomData(*clusterKey1Len, *data1Len, logger) // Generate data for next write
			//logger.Info("003-calling insert")
			writeStartTime := time.Now()
			resultFailed := insertQuery(session, ctx, testrecord.PartitionKey1, testrecord.ClusterKey1, testrecord.Data1, testrecord.Data2, *speculativeRetry, *srNumAttempts, *srTimeoutDelay, qryIdempotentBool, logger)
			writeDuration := time.Since(writeStartTime)
			atomic.AddInt64(&rowAttempt, 1)

			if resultFailed {
				atomic.AddInt64(&errorCnt, 1)
			} else {
				atomic.AddInt64(&successCnt, 1)
			}
			// Track latency for successful writes only
			writeMinLatencyMutex.Lock()
			if writeMinLatency == 0 || writeDuration < writeMinLatency {
				writeMinLatency = writeDuration
			}
			writeMinLatencyMutex.Unlock()
			
			writeMaxLatencyMutex.Lock()
			if writeDuration > writeMaxLatency {
				writeMaxLatency = writeDuration
			}
			writeMaxLatencyMutex.Unlock()
			
			writeTotalLatencyMutex.Lock()
			writeTotalLatency += writeDuration
			writeTotalLatencyMutex.Unlock()
			
			writeLatencyMutex.Lock()
			writeLatencies = append(writeLatencies, writeDuration)
			writeLatencyMutex.Unlock()

			// Log progress inside goroutine
			currentErrorCnt := atomic.LoadInt64(&errorCnt)
			currentSuccessCnt := atomic.LoadInt64(&successCnt)
			currentRowAttempt = currentSuccessCnt + currentErrorCnt
			//currentRowAttempt = atomic.LoadInt64(&rowAttempt)
			if currentRowAttempt%*progressInterval == 0 {
				logger.Info(fmt.Sprintf("Write progress %d rows/%d, Success: %d, Failures: %d, Start Time: %s", 
					currentRowAttempt, *totalWrites, currentSuccessCnt, currentErrorCnt, startTime.Format(time.RFC3339Nano)))
			}
		}(i)
	}

	wg.Wait()

	currentErrorCnt = atomic.LoadInt64(&errorCnt)
	currentSuccessCnt = atomic.LoadInt64(&successCnt)
	//currentRowAttempt = atomic.LoadInt64(&rowAttempt)
	currentRowAttempt = currentSuccessCnt + currentErrorCnt
	writeJobDuration := time.Since(writeJobStartTime)
    writesPerSecond := float64(0)

	// Calculate writes per second
	if writeJobDuration > 0 {
			writesPerSecond = float64(currentSuccessCnt) / writeJobDuration.Seconds()
		}
	// Calculate write latencies
	writeTestResults := calculateWriteLatencies(writeLatencies, currentSuccessCnt, writeTotalLatency, writeMinLatency, writeMaxLatency)

	logger.Info(fmt.Sprintf("WRITE TEST COMPLETED IN %v! Total %d, Success: %d, Failures: %d, Writes/sec: %.2f",
        writeJobDuration, currentRowAttempt, currentSuccessCnt, currentErrorCnt, writesPerSecond))

	// Log write latency results
	logger.Info(fmt.Sprintf("Write latency - Min: %v, Avg: %v, Max: %v, p95: %v, p99: %v", 
        writeTestResults.minLatency, writeTestResults.avgLatency, 
		writeTestResults.maxLatency, writeTestResults.p95Latency, 
		writeTestResults.p99Latency))

	time.Sleep(10 * time.Second)

	if len(testRecords) > 0 && *totalReads > 0 {
		// Run read test with pre-generated test records
		logger.Info(fmt.Sprintf("Starting read test phase with %d reads using %d pre-generated records...", 
			*totalReads, len(testRecords)))

		readStartTime := time.Now()
		readTestResults := runReadTest(session, testRecords, *totalReads, *readConcurrency, *speculativeRetry, *srNumAttempts, *srTimeoutDelay, qryIdempotentBool, *progressInterval, logger)
		readDuration := time.Since(readStartTime)

		// Calculate reads per second
		var readsPerSecond float64
		if readDuration > 0 {
			readsPerSecond = float64(readTestResults.successReads) / readDuration.Seconds()
		}

		getSystemConfig(session, logger)

		logger.Info(fmt.Sprintf("INPUT Number of goroutines: %d", goRoutines))
		logger.Info(fmt.Sprintf("INPUT Consistency: %s", cluster.Consistency))
		logger.Info(fmt.Sprintf("INPUT Max Concurrency: Writes: %d Reads: %d", *writeConcurrency, *readConcurrency))
		logger.Info(fmt.Sprintf("INPUT MaxRequestsPerConn: %d", cluster.MaxRequestsPerConn))
		logger.Info(fmt.Sprintf("INPUT Total Writes: %d Total Reads: %d", *totalWrites, *totalReads))
		logger.Info(fmt.Sprintf("INPUT Connection Timeout: %v Reconnect Interval %v", cluster.ConnectTimeout, cluster.ReconnectInterval))
		logger.Info(fmt.Sprintf("INPUT Speculative Retry: %d Retries: %d Timeout Delay: %d", *speculativeRetry, *srNumAttempts, *srTimeoutDelay))
		logger.Info(fmt.Sprintf("INPUT Timeout: %v WriteTimeout %v", cluster.Timeout, cluster.WriteTimeout))
		logger.Info(fmt.Sprintf("Cluster Config %s", data))

		// Log read test results
		logger.Info(fmt.Sprintf("READ TEST COMPLETED IN %v! Total: %d, Success: %d, Failures: %d, Reads/sec: %.2f", 
			readDuration, readTestResults.totalReads, readTestResults.successReads, 
			readTestResults.failedReads, readsPerSecond))

		logger.Info(fmt.Sprintf("Read latency - Min: %v, Avg: %v, Max: %v, p95: %v, p99: %v", 
			readTestResults.minLatency, readTestResults.avgLatency, 
			readTestResults.maxLatency, readTestResults.p95Latency, 
			readTestResults.p99Latency))
		
	}
	logger.Info(fmt.Sprintf("WRITE TEST COMPLETED IN %v! Total %d, Success: %d, Failures: %d, Writes/sec: %.2f",
        writeJobDuration, currentRowAttempt, currentSuccessCnt, currentErrorCnt, writesPerSecond))

	// Log write latency results
	logger.Info(fmt.Sprintf("Write latency - Min: %v, Avg: %v, Max: %v, p95: %v, p99: %v", 
        writeTestResults.minLatency, writeTestResults.avgLatency, 
		writeTestResults.maxLatency, writeTestResults.p95Latency, 
		writeTestResults.p99Latency))
}
