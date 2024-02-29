// Package main provides a simple HTTP server that interacts with Apache Kafka
// to add and retrieve tasks.
package main

import (
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
)

// Task represents a task with an ID.
type Task struct {
	ID int `json:"id"`
}

// broker is the Kafka broker address.
var broker = []string{"localhost:9092"}

// addToQueue adds a task to the Kafka queue.
func addToQueue(task *Task, broker []string) error {
    // Create a new Kafka configuration.
    config := sarama.NewConfig()
    config.Producer.Return.Successes = true

    // Create a new synchronous producer.
    conn, err := sarama.NewSyncProducer(broker, config)
    if err != nil {
        return fmt.Errorf("Error creating producer: %w", err) // Wrap error for context
    }
    defer conn.Close()

    // Marshal task object to JSON byte array
    taskBytes, err := json.Marshal(task)
    if err != nil {
        return fmt.Errorf("Error marshaling task: %w", err) // Wrap error for context
    }

    // Create a message to be sent to the Kafka topic.
    msg := &sarama.ProducerMessage{
        Topic: "tasks",
        Value: sarama.ByteEncoder(taskBytes),
    }

    // Send the message to the Kafka topic.
    _, _, err = conn.SendMessage(msg)
    if err != nil {
        return fmt.Errorf("Error sending message: %w", err) // Wrap error for context
    }

    fmt.Println("Message sent")
    return nil
}

// getFromQueue retrieves a task from the Kafka queue.
func getFromQueue(broker []string) (*Task, error) {
    // Create a new Kafka configuration.
    config := sarama.NewConfig()
    config.Consumer.Return.Errors = true

    // Create a new consumer.
    conn, err := sarama.NewConsumer(broker, config)
    if err != nil {
        return nil, fmt.Errorf("Error creating consumer: %w", err) 
    }
    defer conn.Close()

    // Consume messages from the "tasks" topic partition.
    partition, err := conn.ConsumePartition("tasks", 0, sarama.OffsetNewest)
    if err != nil {
        return nil, fmt.Errorf("Error consuming partition: %w", err) 
    }
    defer partition.Close()

    // Continuously receive messages from the partition.
    for {
        select {
        case msg := <-partition.Messages():
            var task Task
            err := json.Unmarshal(msg.Value, &task) // Unmarshal byte array into Task object
            if err != nil {
                fmt.Println("Error unmarshaling message:", err) 
                continue
            }
            return &task, nil
        case err := <-partition.Errors():
            fmt.Println("Error consuming message:", err) 
        }
    }
}


func main() {
    app := fiber.New()

    
    app.Post("/task", func(c *fiber.Ctx) error {
task := new(Task)
if err := c.BodyParser(task); err != nil {
	
	return c.Status(400).JSON(fiber.Map{"error": "Invalid task request"})
}

if err := addToQueue(task, broker); err != nil {
	
	fmt.Println("Error adding task to queue:", err)

	return c.Status(500).JSON(fiber.Map{"error": "Internal server error"})
}

        return c.JSON(task)
    })

    app.Get("/task", func(c *fiber.Ctx) error {
        task, err := getFromQueue(broker)
        if err != nil {
            fmt.Println("Error getting task from queue:", err)
            return c.Status(500).JSON(fiber.Map{"error": "Internal server error"})
        }

        return c.JSON(task)
    })

    fmt.Println("Server running on port 3000")
	app.Listen(":3000")

}