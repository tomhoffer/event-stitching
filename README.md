# Event Stitching Service

A Go service that handles event processing and identity stitching. The service processes events with various identifiers (cookies, message IDs, phone numbers) and maintains a unified view of customer identities.

## Features

- Event ingestion with multiple identifiers
- Concurrent event processing
- Identity stitching based on event timestamps
- PostgreSQL storage with JSONB support
- Ginkgo/Gomega test suite

## Prerequisites

- Go 1.23 or later
- PostgreSQL 16 or later
- Docker (optional, for running tests)

## Setup

1. Clone the repository:
```bash
git clone https://github.com/tomhoffer/event-stitching.git
cd event-stitching
```

2. Set up the database (setup.sql)

3. Run the tests:
```bash
go test ./...
```

4. Run the service & the benchmark:
```bash
go run cmd/main.go
```

## License

MIT 