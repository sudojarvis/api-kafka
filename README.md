# Kafka Task API

This is a simple HTTP server built with Go and Fiber that interacts with Apache Kafka to add and retrieve tasks.

## Requirements

- Go
- Fiber
- Sarama

## Installation

1. Install Go: https://golang.org/doc/install
2. Install Fiber: `go get -u github.com/gofiber/fiber/v2`
3. Install Sarama: `go get -u github.com/IBM/sarama`

## Usage

1. Start a Kafka broker on `localhost:9092`.
2. Run the server: `go run main.go`.
3. Use a POST request to add a task: `curl -X POST -d '{"id": 1}' http://localhost:3000/task`.
4. Use a GET request to retrieve a task: `curl http://localhost:3000/task`.

## Configuration

- Kafka broker address: Modify the `broker` variable in `main.go` to point to your Kafka broker.

## API Endpoints

### Add Task

- **URL:** `/task`
- **Method:** POST
- **Request Body:**
  ```json
  {
    "id": 1
  }
  ```
