# Data Migration Import Function Worker Engine

This service consumes import jobs from RabbitMQ, downloads CSV data from a provided API URL, splits the records into batches, and republishes those batches to a downstream queue for database processing.

## What It Does

- Consumes jobs from the `importqueue` RabbitMQ queue.
- Downloads a CSV file from the `api_url` in each job.
- Parses the CSV into JSON-like records using the header row as field names.
- Splits records into batches of `1000`.
- Publishes batch payloads to the `db_jobs` RabbitMQ queue.
- Runs multiple job processors and publisher workers in parallel.
- Prints runtime statistics every 10 seconds.

## Job Flow

1. A producer sends a job message to `importqueue`.
2. This worker reads the job and fetches the CSV file from the remote URL.
3. The CSV rows are converted into objects keyed by CSV header names.
4. Records are grouped into batches.
5. Each batch is published to `db_jobs` for downstream persistence.

## Input Job Payload

Messages consumed from `importqueue` must be JSON in this shape:

```json
{
  "api_url": "https://example.com/data.csv",
  "mongo_uri": "mongodb://localhost:27017",
  "database": "sample_db",
  "collection": "customers"
}
```

## Output Batch Payload

Messages published to `db_jobs` are JSON in this shape:

```json
{
  "db_type": "mongo",
  "conn_str": "mongodb://localhost:27017",
  "database": "sample_db",
  "collection": "customers",
  "data": [
    {
      "id": "1",
      "name": "Alice"
    }
  ]
}
```

## Current Runtime Configuration

These values are currently hard-coded in the application:

- Source queue: `importqueue`
- Target queue: `db_jobs`
- Batch size: `1000`
- Job workers: `10`
- Publisher workers: `10`

## Important Note

The RabbitMQ connection string is not yet configured for real use. In `main.go`, `rabbitConnStr` is still set to a placeholder value:

```go
const rabbitConnStr = "<>"
```

Replace it with a valid RabbitMQ connection string before running the service.

Example:

```go
const rabbitConnStr = "amqp://guest:guest@localhost:5672/"
```

## Prerequisites

- Go `1.24.2` or later
- A running RabbitMQ instance
- A reachable CSV endpoint
- A downstream consumer for the `db_jobs` queue

## Run Locally

Install dependencies and start the worker:

```bash
go mod download
go run .
```

## Build Binary

```bash
go build -o worker-engine .
```

## Run With Docker

Build the image:

```bash
docker build -t datamigration-import-worker .
```

Run the container:

```bash
docker run --rm datamigration-import-worker
```

## Logging And Metrics

The service writes operational logs to standard output, including:

- worker startup messages
- job receipt and batch processing progress
- publisher activity
- processing and publishing errors
- periodic throughput statistics

## Limitations

- Graceful shutdown is incomplete because no OS signal handler currently triggers context cancellation.
- RabbitMQ configuration is hard-coded instead of being loaded from environment variables.
- The Dockerfile is generic and exposes port `8080`, even though this worker does not serve HTTP traffic.

## Project Files

- `main.go`: worker implementation
- `go.mod`: module definition and dependencies
- `Dockerfile`: container build definition# Multi-DB-import-
