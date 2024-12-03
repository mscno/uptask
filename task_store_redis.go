package uptask

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

const (
	// Redis key prefixes
	taskPrefix   = "task:"          // Hash sets storing task details
	timelineKey  = "tasks:timeline" // Sorted set for time-based queries
	statusPrefix = "tasks:status:"  // Sorted sets for status-based queries
)

type RedisTaskStore struct {
	client *redis.Client
}

// RedisConfig holds configuration for Redis-based task store
type RedisConfig struct {
	Addr     string
	Username string
	Password string
	DB       int
	Secure   bool
	// Add any other Redis-specific config you need
}

func NewRedisTaskStore(cfg RedisConfig) (*RedisTaskStore, error) {

	protocol := "redis"
	if cfg.Secure {
		protocol = "rediss"
	}

	opts, err := redis.ParseURL(fmt.Sprintf("%s://%s:%s@%s", protocol, cfg.Username, cfg.Password, cfg.Addr))
	if err != nil {
		return nil, fmt.Errorf("failed to parse Redis URL: %w", err)
	}

	client := redis.NewClient(opts)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	return &RedisTaskStore{client: client}, nil
}

func (s *RedisTaskStore) CreateTaskExecution(ctx context.Context, task *TaskExecution) error {
	// Set created time if not set
	if task.CreatedAt.IsZero() {
		task.CreatedAt = time.Now()
	}

	// Marshal task to JSON
	taskJSON, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	// Create pipeline for atomic operations
	pipe := s.client.Pipeline()

	// Store task details in hash
	taskKey := taskPrefix + task.ID
	pipe.HSet(ctx, taskKey, map[string]interface{}{
		"data":    string(taskJSON),
		"status":  string(task.Status),
		"queue":   task.Queue,
		"created": task.CreatedAt.Unix(),
	})

	// Add to timeline sorted set
	pipe.ZAdd(ctx, timelineKey, redis.Z{
		Score:  float64(task.CreatedAt.Unix()),
		Member: task.ID,
	})

	// Add to status sorted set
	statusKey := statusPrefix + string(task.Status)
	pipe.ZAdd(ctx, statusKey, redis.Z{
		Score:  float64(task.CreatedAt.Unix()),
		Member: task.ID,
	})

	// Execute pipeline
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to create task: %w", err)
	}

	return nil
}

func (s *RedisTaskStore) GetTaskExecution(ctx context.Context, taskID string) (*TaskExecution, error) {
	taskKey := taskPrefix + taskID

	// Get task JSON from hash
	taskJSON, err := s.client.HGet(ctx, taskKey, "data").Result()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("task not found: %s", taskID)
		}
		return nil, fmt.Errorf("failed to get task: %w", err)
	}

	var task TaskExecution
	if err := json.Unmarshal([]byte(taskJSON), &task); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task: %w", err)
	}

	return &task, nil
}

func (s *RedisTaskStore) TaskExists(ctx context.Context, taskID string) (bool, error) {
	taskKey := taskPrefix + taskID
	keys, err := s.client.Exists(ctx, taskKey).Result()
	if err != nil {
		return false, err
	}
	return keys == 1, nil
}

func (s *RedisTaskStore) UpdateTaskStatus(ctx context.Context, taskID string, status TaskStatus) error {
	task, err := s.GetTaskExecution(ctx, taskID)
	if err != nil {
		return err
	}

	// Update task status and timing
	oldStatus := task.Status
	task.Status = status
	if status == TaskStatusRunning {
		task.AttemptedAt = time.Now()
	}

	if status == TaskStatusSuccess || status == TaskStatusFailed {
		task.FinalizedAt = time.Now()
	}

	if status == TaskStatusPending {
		task.Attempt++
	}

	// Marshal updated task
	taskJSON, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	pipe := s.client.Pipeline()

	// Update task hash
	taskKey := taskPrefix + taskID
	pipe.HSet(ctx, taskKey, map[string]interface{}{
		"data":   string(taskJSON),
		"status": string(status),
	})

	// Remove from old status set and add to new status set
	oldStatusKey := statusPrefix + string(oldStatus)
	newStatusKey := statusPrefix + string(status)
	pipe.ZRem(ctx, oldStatusKey, taskID)
	pipe.ZAdd(ctx, newStatusKey, redis.Z{
		Score:  float64(task.CreatedAt.Unix()),
		Member: taskID,
	})

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to update task status: %w", err)
	}

	return nil
}

func (s *RedisTaskStore) UpdateTaskSnoozedTask(ctx context.Context, taskID string, scheduledAt time.Time) error {
	task, err := s.GetTaskExecution(ctx, taskID)
	if err != nil {
		return err
	}
	oldStatus := task.Status
	task.Status = TaskStatusPending
	task.ScheduledAt = scheduledAt
	task.Attempt--

	// Marshal updated task
	taskJSON, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	// Update task hash
	pipe := s.client.Pipeline()

	// Update task hash
	taskKey := taskPrefix + taskID
	pipe.HSet(ctx, taskKey, map[string]interface{}{
		"data":   string(taskJSON),
		"status": string(task.Status),
	})

	// Remove from old status set and add to new status set
	oldStatusKey := statusPrefix + string(oldStatus)
	newStatusKey := statusPrefix + string(task.Status)
	pipe.ZRem(ctx, oldStatusKey, taskID)
	pipe.ZAdd(ctx, newStatusKey, redis.Z{
		Score:  float64(task.CreatedAt.Unix()),
		Member: taskID,
	})

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to update task status: %w", err)
	}

	return nil
}

func (s *RedisTaskStore) DeleteTaskExecution(ctx context.Context, taskID string) error {
	// Get the task first to check existence and get status
	taskKey := taskPrefix + taskID
	task, err := s.GetTaskExecution(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task for deletion: %w", err)
	}

	// Create pipeline for atomic operations
	pipe := s.client.Pipeline()

	// Remove from timeline sorted set
	pipe.ZRem(ctx, timelineKey, taskID)

	// Remove from status sorted set
	statusKey := statusPrefix + string(task.Status)
	pipe.ZRem(ctx, statusKey, taskID)

	// Delete the hash
	pipe.Del(ctx, taskKey)

	// Execute pipeline
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to delete task: %w", err)
	}

	return nil
}

func (s *RedisTaskStore) AddTaskError(ctx context.Context, taskID string, taskError TaskError) error {
	task, err := s.GetTaskExecution(ctx, taskID)
	if err != nil {
		return err
	}

	// Set error timestamp if not set
	if taskError.Timestamp.IsZero() {
		taskError.Timestamp = time.Now()
	}

	// Append error to task
	task.Errors = append(task.Errors, taskError)

	// Marshal updated task
	taskJSON, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	// Update task hash
	taskKey := taskPrefix + taskID
	err = s.client.HSet(ctx, taskKey, "data", string(taskJSON)).Err()
	if err != nil {
		return fmt.Errorf("failed to add task error: %w", err)
	}

	return nil
}

func (s *RedisTaskStore) ListTaskExecutions(ctx context.Context, filter TaskFilter) ([]*TaskExecution, error) {
	var taskIDs []string
	var err error

	if filter.Status != nil {
		// If status is specified, get from status set
		statusKey := statusPrefix + string(*filter.Status)
		opt := &redis.ZRangeBy{
			Min:    "-inf",
			Max:    "+inf",
			Offset: 0,
			Count:  int64(filter.Limit),
		}
		taskIDs, err = s.client.ZRevRangeByScore(ctx, statusKey, opt).Result()
	} else {
		if filter.ToDate.IsZero() {
			filter.ToDate = time.Now()
		}
		// Otherwise, get from timeline
		opt := &redis.ZRangeBy{
			Min:    fmt.Sprint(filter.FromDate.Unix()),
			Max:    fmt.Sprint(filter.ToDate.Unix()),
			Offset: 0,
			Count:  int64(filter.Limit),
		}
		taskIDs, err = s.client.ZRevRangeByScore(ctx, timelineKey, opt).Result()
	}

	if err != nil {
		return nil, fmt.Errorf("failed to list tasks: %w", err)
	}

	// Fetch tasks in parallel using pipelining
	pipe := s.client.Pipeline()
	cmds := make(map[string]*redis.StringCmd)

	for _, taskID := range taskIDs {
		taskKey := taskPrefix + taskID
		cmds[taskID] = pipe.HGet(ctx, taskKey, "data")
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch tasks: %w", err)
	}

	// Process results
	tasks := make([]*TaskExecution, 0, len(taskIDs))
	for _, taskID := range taskIDs {
		taskJSON, err := cmds[taskID].Result()
		if err != nil {
			continue // Skip failed tasks
		}

		var task TaskExecution
		if err := json.Unmarshal([]byte(taskJSON), &task); err != nil {
			continue // Skip invalid JSON
		}

		tasks = append(tasks, &task)
	}

	return tasks, nil
}

func (s *RedisTaskStore) GetMostRecentTaskExecutions(ctx context.Context, limit int) ([]*TaskExecution, error) {
	return s.ListTaskExecutions(ctx, TaskFilter{
		FromDate: time.Unix(0, 0),
		ToDate:   time.Now(),
		Limit:    limit,
	})
}

func (s *RedisTaskStore) CleanupOldTaskExecutions(ctx context.Context, olderThan time.Duration) error {
	cutoff := time.Now().Add(-olderThan).Unix()

	// Get old task IDs from timeline
	oldTaskIDs, err := s.client.ZRangeByScore(ctx, timelineKey, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   fmt.Sprint(cutoff),
		Count: 1000, // Process in batches
	}).Result()
	if err != nil {
		return fmt.Errorf("failed to get old tasks: %w", err)
	}

	if len(oldTaskIDs) == 0 {
		return nil
	}

	pipe := s.client.Pipeline()

	// Remove tasks from all relevant keys
	for _, taskID := range oldTaskIDs {
		taskKey := taskPrefix + taskID

		// Get task status to remove from status set
		status, err := s.client.HGet(ctx, taskKey, "status").Result()
		if err == nil {
			statusKey := statusPrefix + status
			pipe.ZRem(ctx, statusKey, taskID)
		}

		// Remove task hash and timeline entry
		pipe.Del(ctx, taskKey)
		pipe.ZRem(ctx, timelineKey, taskID)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to cleanup old tasks: %w", err)
	}

	return nil
}
