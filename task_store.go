package uptask

import (
	"context"
	"time"
)

// TaskStatus represents the current state of a task execution
type TaskStatus string

const (
	TaskStatusPending TaskStatus = "PENDING"
	TaskStatusRunning TaskStatus = "RUNNING"
	TaskStatusSuccess TaskStatus = "SUCCESS"
	TaskStatusFailed  TaskStatus = "FAILED"
)

// TaskExecution represents a single execution attempt of a task
type TaskExecution struct {
	// Core fields
	ID       string      `json:"id"`
	TaskKind string      `json:"task_kind"`
	Status   TaskStatus  `json:"status"`
	Args     interface{} `json:"args"`

	// Attempt tracking
	AttemptID   string `json:"attempt_id"`
	Attempt     int    `json:"attempt"`
	MaxAttempts int    `json:"max_attempts"`

	// External references
	QstashMessageID string `json:"qstash_message_id,omitempty"`
	ScheduleID      string `json:"schedule_id,omitempty"`

	// Timing information
	CreatedAt   time.Time `json:"created_at"`
	AttemptedAt time.Time `json:"attempted_at,omitempty"`
	ScheduledAt time.Time `json:"scheduled_at"`
	FinalizedAt time.Time `json:"finalized_at,omitempty"`

	// Error tracking
	Errors []TaskError `json:"errors,omitempty"`
	Queue  string      `json:"queue"`
}

// TaskError represents an error that occurred during task execution
type TaskError struct {
	Message   string                 `json:"message"`
	Details   map[string]interface{} `json:"details,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
}

// TaskFilter provides options for filtering task lists
type TaskFilter struct {
	Status   *TaskStatus
	Queue    string
	FromDate time.Time
	ToDate   time.Time
	Limit    int
}

// TaskStore defines the interface for task storage operations
type TaskStore interface {
	// Core operations
	CreateTaskExecution(ctx context.Context, task *TaskExecution) error
	GetTaskExecution(ctx context.Context, taskID string) (*TaskExecution, error)
	DeleteTaskExecution(ctx context.Context, taskID string) error
	UpdateTaskStatus(ctx context.Context, taskID string, status TaskStatus) error
	UpdateTaskScheduledAt(ctx context.Context, taskID string, scheduledAt time.Time) error

	AddTaskError(ctx context.Context, taskID string, err TaskError) error

	// Query operations
	ListTaskExecutions(ctx context.Context, filter TaskFilter) ([]*TaskExecution, error)
	GetMostRecentTaskExecutions(ctx context.Context, limit int) ([]*TaskExecution, error)

	// Optional: Cleanup operation if needed
	CleanupOldTaskExecutions(ctx context.Context, olderThan time.Duration) error
}
