package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"github.com/mscno/uptask"
	"github.com/mscno/uptask/internal/slogger"
	"github.com/mscno/uptask/uptaskhttp"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"time"
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

// GetAnimalFromUUID hashes a UUID string to an animal name
// GetAnimalWithNumberFromUUID hashes a UUID string to an animal name with a consistent 2-digit number
func GetAnimalWithNumberFromUUID(uuidString string) string {
	// Define a list of animals for easier debugging and visualization
	animals := []string{
		"Aardvark", "Albatross", "Alligator", "Alpaca", "Anteater",
		"Armadillo", "Baboon", "Badger", "Barracuda", "Bat",
		"Bear", "Beaver", "Bee", "Bison", "Buffalo",
		"Butterfly", "Camel", "Capybara", "Caribou", "Cassowary",
		"Cheetah", "Chicken", "Chimpanzee", "Chinchilla", "Cobra",
		"Coyote", "Crab", "Crocodile", "Crow", "Deer",
		"Dingo", "Dolphin", "Dragonfly", "Duck", "Eagle",
		"Echidna", "Elephant", "Emu", "Falcon", "Ferret",
		"Flamingo", "Fox", "Frog", "Gazelle", "Giraffe",
		"Goat", "Gorilla", "Grasshopper", "Hamster", "Hawk",
		"Hedgehog", "Hippopotamus", "Honeybee", "Hyena", "Iguana",
		"Jackal", "Jaguar", "Jellyfish", "Kangaroo", "Koala",
	}

	// Create a hash of the UUID
	hash := sha256.Sum256([]byte(uuidString))

	// Use first 8 bytes of hash as a uint64 for the animal
	hashValue := binary.BigEndian.Uint64(hash[:8])

	// Use the next 8 bytes for the number (0-99)
	numberValue := binary.BigEndian.Uint64(hash[8:16]) % 100

	// Map the hash to an animal (modulo operation ensures it fits within array bounds)
	animalIndex := hashValue % uint64(len(animals))

	// Format with a consistent 2-digit number (padded with leading zero if necessary)
	return fmt.Sprintf("%s-%02d", animals[animalIndex], numberValue)
}

func (p *DummyTaskProcessor) ProcessTask(ctx context.Context, task *uptask.Task[DummyTask]) error {
	slog.Info(fmt.Sprintf("Processing task %s - Retries (%d/%d) - ID: %s", GetAnimalWithNumberFromUUID(task.Id), task.Retried, task.MaxRetries, task.Id))
	var secs int

	var secsToSleep = rand.IntN(8)
	//slog.Info("sleeping for ", "secs", secsToSleep)
	for {
		if secs > secsToSleep {
			break
		}

		// Check if context is done before sleeping
		select {
		case <-ctx.Done():
			slog.Error("Task cancelled due to timeout " + GetAnimalWithNumberFromUUID(task.Id))
			return ctx.Err() // Return the context error
		default:
			// Continue processing
		}

		time.Sleep(1 * time.Second)
		//fmt.Println("Sleeping", task.Id, secs)
		secs++
	}

	if task.Retried < 1 {
		return uptask.JobSnooze(time.Second * 5)
	}

	slog.Info(fmt.Sprintf("Task completed %s - Retries (%d/%d) - ID: %s", GetAnimalWithNumberFromUUID(task.Id), task.Retried, task.MaxRetries, task.Id))
	return nil
}

func (p *DummyTaskProcessor) Timeout(task *uptask.Task[DummyTask]) time.Duration {
	return 5 * time.Second
}

func main() {
	l := slogger.NewSlogger(true)
	qstashToken := os.Getenv("QSTASH_TOKEN")
	if qstashToken == "" {
		fmt.Println("QSTASH_TOKEN environment variable is required")
		os.Exit(1)
	}
	qstashSigningKey := os.Getenv("QSTASH_SIGNING_KEY")
	if qstashSigningKey == "" {
		fmt.Println("QSTASH_SIGNING_KEY environment variable is required")
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
	service := uptask.NewTaskService(transport, uptask.WithStore(store), uptask.WithLogger(l))
	uptask.AddTaskHandler[DummyTask](service, &DummyTaskProcessor{})
	mux.HandleFunc("POST /", uptaskhttp.HandleTasks(service))
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write(bytes.NewBufferString("Hello World").Bytes())
	})
	fmt.Println("Starting server on :8180")
	err = http.ListenAndServe(":8180", mux)
	if err != nil {
		fmt.Println("Failed to start server", err)
		os.Exit(1)
	}
}
