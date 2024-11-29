package main

import (
	"context"
	"fmt"
	"github.com/mscno/uptask"
	"github.com/mscno/uptask/uptaskhttp"
	"net/http"
	"os"
)

type DummyTask struct {
	Name string
}

func (t DummyTask) Kind() string {
	return "DummyTask"
}

type DummyTaskProcessor struct {
	uptask.TaskHandlerDefaults[DummyTask]
}

func (p *DummyTaskProcessor) ProcessTask(ctx context.Context, task *uptask.Task[DummyTask]) error {
	fmt.Println("Processing task", task)
	if task.Attempt == 1 {
		fmt.Println("Failing task", task)
		return fmt.Errorf("failed task")
	}
	fmt.Println("Task completed", task)
	return nil
}

func main() {
	qstashToken := os.Getenv("QSTASH_TOKEN")
	if qstashToken == "" {
		fmt.Println("QSTASH_TOKEN environment variable is required")
		os.Exit(1)
	}
	jobUrl := os.Getenv("JOB_URL")
	if jobUrl == "" {
		fmt.Println("JOB_URL environment variable is required")
		os.Exit(1)
	}
	redisUrl := os.Getenv("REDIS_URL")
	if redisUrl == "" {
		fmt.Println("REDIS_URL environment variable is required")
		os.Exit(1)
	}
	redisPassword := os.Getenv("REDIS_PASSWORD")
	if redisPassword == "" {
		fmt.Println("REDIS_PASSWORD environment variable is required")
		os.Exit(1)
	}
	redisPort := os.Getenv("REDIS_PORT")
	if redisPort == "" {
		redisPort = "6379"
	}

	mux := http.NewServeMux()
	transport := uptask.NewUpstashTransport(qstashToken, jobUrl)
	store, err := uptask.NewRedisTaskStore(uptask.RedisConfig{
		Addr:     fmt.Sprintf("%s:%s", redisUrl, redisPort),
		Username: "default",
		Password: redisPassword,
		Secure:   true,
	})
	if err != nil {
		fmt.Println("Failed to create Redis task store", err)
		os.Exit(1)
	}
	service := uptask.NewTaskService(transport, uptask.WithStore(store))
	uptask.AddTaskHandler[DummyTask](service, &DummyTaskProcessor{})
	mux.HandleFunc("POST /", uptaskhttp.HandleTasks(service))
	fmt.Println("Starting server on :8080")
	err = http.ListenAndServe(":8080", mux)
	if err != nil {
		fmt.Println("Failed to start server", err)
		os.Exit(1)
	}
}
