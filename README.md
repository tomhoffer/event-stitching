# Event Stitching Service

A dummy Golang service for resolving identity of customers performing actions on e-commerce websites. The service processes events with various identifiers (cookies, message IDs, phone numbers) and maintains a unified view of customer identities.

## Features

- Event ingestion, where event represents an action performed by the customer (e.g. page view, purchase of some product, adding a product into basket)
- Concurrent event processing
- Concurrent merging of customer profiles which share common identifiers (cookie, email, phone)
- PostgreSQL storage
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