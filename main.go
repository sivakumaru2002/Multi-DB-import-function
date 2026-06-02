package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type JobPayload struct {
	BlobAPIURL string `json:"api_url"`
	MongoURI   string `json:"mongo_uri"`
	Database   string `json:"database"`
	Collection string `json:"collection"`
}

type WorkerPayload struct {
	DBType     string        `json:"db_type"`
	ConnStr    string        `json:"conn_str"`
	Database   string        `json:"database"`
	Collection string        `json:"collection"`
	Table      string        `json:"table,omitempty"`
	Data       []interface{} `json:"data"`
}

type Stats struct {
	TotalJobs        int64
	TotalBatches     int64
	TotalRecords     int64
	PublishErrors    int64
	ProcessingErrors int64
}

const (
	sourceQueueName    = "importqueue"
	targetQueueName    = "db_jobs"
	batchSize          = 1000
	workerCount        = 10
	publishWorkerCount = 10   // Optimal for most use cases
	rabbitConnStr      = "<>" // Replace with your RabbitMQ connection string
)

var stats Stats

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Println("🚀 Starting Parallel RabbitMQ Batch Publisher...")
	fmt.Printf("📊 Configuration: %d workers, %d publishers, batch size: %d\n",
		workerCount, publishWorkerCount, batchSize)

	// Create publisher connection pool
	publisherPool, err := createPublisherPool(publishWorkerCount)
	if err != nil {
		panic(fmt.Errorf("failed to create publisher pool: %w", err))
	}
	defer closePublisherPool(publisherPool)

	// Main consumer connection
	mainConn, mainCh, err := createConnection()
	if err != nil {
		panic(fmt.Errorf("failed to create main connection: %w", err))
	}
	defer mainConn.Close()
	defer mainCh.Close()

	// Setup consumer
	msgs, err := setupConsumer(mainCh)
	if err != nil {
		panic(fmt.Errorf("failed to setup consumer: %w", err))
	}

	// Channels for communication
	jobChan := make(chan JobPayload, 20)
	batchChan := make(chan WorkerPayload, 500) // Large buffer for high throughput

	// Start statistics reporter
	go reportStats(ctx)

	// Start job listener
	go listenForJobs(ctx, msgs, jobChan)

	// Start job processors
	var processorWg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		processorWg.Add(1)
		go func(workerID int) {
			defer processorWg.Done()
			processJobs(ctx, jobChan, batchChan, workerID)
		}(i)
	}

	// Start parallel publishers
	var publisherWg sync.WaitGroup
	for i := 0; i < publishWorkerCount; i++ {
		publisherWg.Add(1)
		go func(publisherID int) {
			defer publisherWg.Done()
			publishBatches(ctx, publisherPool[publisherID], batchChan, publisherID)
		}(i)
	}

	// Wait for shutdown signal or context cancellation
	fmt.Println("✅ All workers started. Press Ctrl+C to shutdown...")
	<-ctx.Done()

	fmt.Println("🔄 Shutting down gracefully...")
	close(jobChan)
	processorWg.Wait()

	close(batchChan)
	publisherWg.Wait()

	fmt.Println("✅ Shutdown complete")
	printFinalStats()
}

func createConnection() (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(rabbitConnStr)
	if err != nil {
		return nil, nil, fmt.Errorf("connection failed: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("channel creation failed: %w", err)
	}

	// Set QoS for better performance
	err = ch.Qos(100, 0, false)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, fmt.Errorf("QoS setup failed: %w", err)
	}

	return conn, ch, nil
}

func createPublisherPool(poolSize int) ([]*amqp.Channel, error) {
	pool := make([]*amqp.Channel, poolSize)

	for i := 0; i < poolSize; i++ {
		_, ch, err := createConnection()
		if err != nil {
			// Cleanup already created channels
			for j := 0; j < i; j++ {
				pool[j].Close()
			}
			return nil, fmt.Errorf("failed to create publisher %d: %w", i, err)
		}

		// Declare queues for each publisher
		_, err = ch.QueueDeclare(targetQueueName, true, false, false, false, nil)
		if err != nil {
			ch.Close()
			return nil, fmt.Errorf("queue declaration failed for publisher %d: %w", i, err)
		}

		pool[i] = ch
		fmt.Printf("✅ Publisher %d connection established\n", i)
	}

	return pool, nil
}

func closePublisherPool(pool []*amqp.Channel) {
	for i, ch := range pool {
		if ch != nil {
			ch.Close()
			fmt.Printf("🔒 Publisher %d connection closed\n", i)
		}
	}
}

func setupConsumer(ch *amqp.Channel) (<-chan amqp.Delivery, error) {
	// Declare queues
	_, err := ch.QueueDeclare(sourceQueueName, true, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("source queue declaration failed: %w", err)
	}

	_, err = ch.QueueDeclare(targetQueueName, true, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("target queue declaration failed: %w", err)
	}

	msgs, err := ch.Consume(
		sourceQueueName,
		"",    // consumer tag
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return nil, fmt.Errorf("consume setup failed: %w", err)
	}

	return msgs, nil
}

func listenForJobs(ctx context.Context, msgs <-chan amqp.Delivery, jobChan chan<- JobPayload) {
	fmt.Println("📥 Job listener started...")

	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				fmt.Println("📥 Message channel closed")
				return
			}

			var job JobPayload
			if err := json.Unmarshal(msg.Body, &job); err != nil {
				atomic.AddInt64(&stats.ProcessingErrors, 1)
				fmt.Printf("❌ Failed to decode job: %v\n", err)
				continue
			}

			atomic.AddInt64(&stats.TotalJobs, 1)
			fmt.Printf("📦 Received job %d: %s\n", stats.TotalJobs, job.BlobAPIURL)

			select {
			case jobChan <- job:
			case <-ctx.Done():
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

func processJobs(ctx context.Context, jobChan <-chan JobPayload, batchChan chan<- WorkerPayload, workerID int) {
	fmt.Printf("🔧 Job processor %d started\n", workerID)

	for {
		select {
		case job, ok := <-jobChan:
			if !ok {
				fmt.Printf("🔧 Job processor %d: channel closed\n", workerID)
				return
			}

			if err := processJob(ctx, job, batchChan, workerID); err != nil {
				atomic.AddInt64(&stats.ProcessingErrors, 1)
				fmt.Printf("❌ Worker %d: Job processing failed: %v\n", workerID, err)
			}

		case <-ctx.Done():
			return
		}
	}
}

func processJob(ctx context.Context, job JobPayload, batchChan chan<- WorkerPayload, workerID int) error {
	startTime := time.Now()

	// Download CSV
	csvData, err := downloadCSV(job.BlobAPIURL)
	if err != nil {
		return fmt.Errorf("CSV download failed: %w", err)
	}

	// Parse CSV
	records, err := parseCSV(csvData)
	if err != nil {
		return fmt.Errorf("CSV parsing failed: %w", err)
	}

	totalRecords := len(records)
	batchCount := (totalRecords + batchSize - 1) / batchSize // Ceiling division

	fmt.Printf("🔧 Worker %d: Processing %d records into %d batches\n",
		workerID, totalRecords, batchCount)

	// Create and send batches
	for i := 0; i < totalRecords; i += batchSize {
		end := i + batchSize
		if end > totalRecords {
			end = totalRecords
		}

		batchRecords := records[i:end]
		data := make([]interface{}, len(batchRecords))
		for j, record := range batchRecords {
			data[j] = record
		}

		batchPayload := WorkerPayload{
			DBType:     "mongo",
			ConnStr:    job.MongoURI,
			Database:   job.Database,
			Collection: job.Collection,
			Data:       data,
		}

		select {
		case batchChan <- batchPayload:
			atomic.AddInt64(&stats.TotalRecords, int64(len(data)))
		case <-time.After(10 * time.Second):
			return fmt.Errorf("timeout sending batch to channel")
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	duration := time.Since(startTime)
	fmt.Printf("✅ Worker %d: Completed job in %v (%d batches, %d records)\n",
		workerID, duration, batchCount, totalRecords)

	return nil
}

func publishBatches(ctx context.Context, ch *amqp.Channel, batchChan <-chan WorkerPayload, publisherID int) {
	fmt.Printf("📤 Publisher %d started\n", publisherID)
	batchCount := int64(0)

	for {
		select {
		case batch, ok := <-batchChan:
			if !ok {
				fmt.Printf("📤 Publisher %d: channel closed (%d batches sent)\n", publisherID, batchCount)
				return
			}

			if err := publishBatch(ctx, ch, batch, publisherID); err != nil {
				atomic.AddInt64(&stats.PublishErrors, 1)
				fmt.Printf("❌ Publisher %d: Failed to publish batch: %v\n", publisherID, err)
				continue
			}

			batchCount++
			atomic.AddInt64(&stats.TotalBatches, 1)

			if batchCount%100 == 0 {
				fmt.Printf("📤 Publisher %d: Sent %d batches\n", publisherID, batchCount)
			}

		case <-ctx.Done():
			return
		}
	}
}

func publishBatch(ctx context.Context, ch *amqp.Channel, batch WorkerPayload, publisherID int) error {
	payload, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}

	publishCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	err = ch.PublishWithContext(publishCtx,
		"", // exchange
		targetQueueName,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         payload,
			Timestamp:    time.Now(),
			MessageId:    fmt.Sprintf("batch_%d_%d_%d", publisherID, stats.TotalBatches, time.Now().Unix()),
			DeliveryMode: amqp.Persistent, // Make messages persistent
		},
	)

	return err
}

func downloadCSV(url string) ([]byte, error) {
	client := &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("HTTP GET failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response failed: %w", err)
	}

	return data, nil
}

func parseCSV(data []byte) ([]map[string]interface{}, error) {
	reader := csv.NewReader(bytes.NewReader(data))
	reader.FieldsPerRecord = -1 // Allow variable number of fields
	reader.TrimLeadingSpace = true

	// Read headers
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("reading headers failed: %w", err)
	}

	// Trim whitespace from headers
	for i, header := range headers {
		headers[i] = string(bytes.TrimSpace([]byte(header)))
	}

	var records []map[string]interface{}
	rowNum := 1

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("⚠️ Skipping malformed row %d: %v\n", rowNum+1, err)
			rowNum++
			continue
		}

		record := make(map[string]interface{})
		for i, header := range headers {
			if i < len(row) {
				record[string(header)] = row[i]
			} else {
				record[string(header)] = ""
			}
		}

		records = append(records, record)
		rowNum++
	}

	return records, nil
}

func reportStats(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	var lastBatches, lastRecords int64
	startTime := time.Now()

	for {
		select {
		case <-ticker.C:
			currentBatches := atomic.LoadInt64(&stats.TotalBatches)
			currentRecords := atomic.LoadInt64(&stats.TotalRecords)

			batchesPerSec := float64(currentBatches-lastBatches) / 10.0
			recordsPerSec := float64(currentRecords-lastRecords) / 10.0

			fmt.Printf("\n📊 STATS (Runtime: %v)\n", time.Since(startTime).Round(time.Second))
			fmt.Printf("   Jobs: %d | Batches: %d (%.1f/s) | Records: %d (%.1f/s)\n",
				atomic.LoadInt64(&stats.TotalJobs),
				currentBatches, batchesPerSec,
				currentRecords, recordsPerSec)
			fmt.Printf("   Errors: Processing=%d, Publishing=%d\n",
				atomic.LoadInt64(&stats.ProcessingErrors),
				atomic.LoadInt64(&stats.PublishErrors))
			fmt.Println("----------------------------------------")

			lastBatches = currentBatches
			lastRecords = currentRecords

		case <-ctx.Done():
			return
		}
	}
}

func printFinalStats() {
	fmt.Println("\n🏁 FINAL STATISTICS")
	fmt.Printf("   Total Jobs Processed: %d\n", stats.TotalJobs)
	fmt.Printf("   Total Batches Sent: %d\n", stats.TotalBatches)
	fmt.Printf("   Total Records Processed: %d\n", stats.TotalRecords)
	fmt.Printf("   Processing Errors: %d\n", stats.ProcessingErrors)
	fmt.Printf("   Publishing Errors: %d\n", stats.PublishErrors)

	if stats.TotalBatches > 0 {
		errorRate := float64(stats.PublishErrors) / float64(stats.TotalBatches) * 100
		fmt.Printf("   Success Rate: %.2f%%\n", 100-errorRate)
	}
}

// {
//   "api_url": "https://terraformstrg.blob.core.windows.net/logs/Import_Test.csv?sp=r&st=2025-11-11T04:41:15Z&se=2026-04-17T12:56:15Z&spr=https&sv=2024-11-04&sr=b&sig=%2BXPXpvGo%2FpO6zSnwk63kD3%2BVZZUv%2BSGPdEugRmC%2B1zM%3D",
//   "mongo_uri": "mongodb+srv://siva:Siva2002@cluster0.0vskyrd.mongodb.net/siva",
//   "database": "siva",
//   "collection": "importtest"
// }
