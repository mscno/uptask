package uptask

import (
	"context"
	"fmt"
	"github.com/alicebob/miniredis/v2"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestRedis(t *testing.T) (*RedisTaskStore, *miniredis.Miniredis) {
	mr, err := miniredis.Run()
	require.NoError(t, err)

	store, err := NewRedisTaskStore(RedisConfig{
		Addr: mr.Addr(),
	})
	require.NoError(t, err)

	return store, mr
}

func createTestTask(id string) *TaskExecution {
	now := time.Now()
	return &TaskExecution{
		ID:              id,
		Status:          TaskStatusPending,
		Args:            map[string]interface{}{"test": "value"},
		AttemptID:       "attempt1",
		Retried:         1,
		MaxRetries:      3,
		QstashMessageID: "qstash1",
		ScheduleID:      "schedule1",
		CreatedAt:       now,
		ScheduledAt:     now.Add(time.Hour),
		Queue:           "default",
	}
}

func TestNewRedisTaskStore(t *testing.T) {
	t.Run("successful connection", func(t *testing.T) {
		mr, err := miniredis.Run()
		require.NoError(t, err)
		defer mr.Close()

		store, err := NewRedisTaskStore(RedisConfig{
			Addr: mr.Addr(),
		})
		assert.NoError(t, err)
		assert.NotNil(t, store)
	})

	t.Run("failed connection", func(t *testing.T) {
		store, err := NewRedisTaskStore(RedisConfig{
			Addr: "invalid:6379",
		})
		assert.Error(t, err)
		assert.Nil(t, store)
	})
}

func TestCreateTaskExecution(t *testing.T) {
	store, mr := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()

	t.Run("successful creation", func(t *testing.T) {
		task := createTestTask("task1")
		err := store.CreateTaskExecution(ctx, task)
		assert.NoError(t, err)

		// Verify task exists in Redis
		taskKey := "task:task1"
		assert.True(t, mr.Exists(taskKey))

		// Verify task in timeline
		assert.True(t, mr.Exists("tasks:timeline"))

		// Verify task in status set
		statusKey := "tasks:status:PENDING"
		assert.True(t, mr.Exists(statusKey))
	})

	t.Run("duplicate task", func(t *testing.T) {
		task := createTestTask("task1")
		err := store.CreateTaskExecution(ctx, task)
		assert.NoError(t, err) // Redis will overwrite existing hash
	})
}

func TestGetTaskExecution(t *testing.T) {
	store, mr := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()
	task := createTestTask("task1")

	t.Run("get existing task", func(t *testing.T) {
		// Create task first
		err := store.CreateTaskExecution(ctx, task)
		require.NoError(t, err)

		// Get task
		retrieved, err := store.GetTaskExecution(ctx, "task1")
		assert.NoError(t, err)
		assert.Equal(t, task.ID, retrieved.ID)
		assert.Equal(t, task.Status, retrieved.Status)
		assert.Equal(t, task.Queue, retrieved.Queue)
	})

	t.Run("get non-existent task", func(t *testing.T) {
		retrieved, err := store.GetTaskExecution(ctx, "nonexistent")
		assert.Error(t, err)
		assert.Nil(t, retrieved)
	})
}

func TestUpdateTaskStatus(t *testing.T) {
	store, mr := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()
	task := createTestTask("task1")

	t.Run("successful status update", func(t *testing.T) {
		// Create task first
		err := store.CreateTaskExecution(ctx, task)
		require.NoError(t, err)

		// Update status
		err = store.UpdateTaskStatus(ctx, "task1", TaskStatusRunning)
		assert.NoError(t, err)

		// Verify status change
		retrieved, err := store.GetTaskExecution(ctx, "task1")
		assert.NoError(t, err)
		assert.Equal(t, TaskStatusRunning, retrieved.Status)

		// Verify status sets updated
		assert.False(t, mr.Exists("tasks:status:PENDING"))
		assert.True(t, mr.Exists("tasks:status:RUNNING"))
	})

	t.Run("update non-existent task", func(t *testing.T) {
		err := store.UpdateTaskStatus(ctx, "nonexistent", TaskStatusRunning)
		assert.Error(t, err)
	})

	t.Run("update to final status", func(t *testing.T) {
		task := createTestTask("task2")
		err := store.CreateTaskExecution(ctx, task)
		require.NoError(t, err)

		err = store.UpdateTaskStatus(ctx, "task2", TaskStatusSuccess)
		assert.NoError(t, err)

		retrieved, err := store.GetTaskExecution(ctx, "task2")
		assert.NoError(t, err)
		assert.False(t, retrieved.FinalizedAt.IsZero())
	})
}

func TestSnoozeTaskStatus(t *testing.T) {
	store, mr := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()
	task := createTestTask("task1")

	t.Run("successful status update", func(t *testing.T) {
		// Create task first
		err := store.CreateTaskExecution(ctx, task)
		require.NoError(t, err)

		// Update status
		err = store.UpdateTaskStatus(ctx, "task1", TaskStatusRunning)
		assert.NoError(t, err)

		// Update status to snoozed
		now := time.Now().Truncate(time.Second)
		err = store.UpdateTaskSnoozedTask(ctx, "task1", now)
		assert.NoError(t, err)

		// Verify status change
		retrieved, err := store.GetTaskExecution(ctx, "task1")
		assert.NoError(t, err)
		assert.Equal(t, TaskStatusPending, retrieved.Status)
		assert.Equal(t, now.UTC(), retrieved.ScheduledAt.UTC())

		// Verify status sets updated
		assert.True(t, mr.Exists("tasks:status:PENDING"))
		assert.False(t, mr.Exists("tasks:status:RUNNING"))
	})

	t.Run("update non-existent task", func(t *testing.T) {
		err := store.UpdateTaskStatus(ctx, "nonexistent", TaskStatusRunning)
		assert.Error(t, err)
	})

	t.Run("update to final status", func(t *testing.T) {
		task := createTestTask("task2")
		err := store.CreateTaskExecution(ctx, task)
		require.NoError(t, err)

		err = store.UpdateTaskStatus(ctx, "task2", TaskStatusSuccess)
		assert.NoError(t, err)

		retrieved, err := store.GetTaskExecution(ctx, "task2")
		assert.NoError(t, err)
		assert.False(t, retrieved.FinalizedAt.IsZero())
	})
}

func TestAddTaskError(t *testing.T) {
	store, mr := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()
	task := createTestTask("task1")

	t.Run("add error to existing task", func(t *testing.T) {
		// Create task first
		err := store.CreateTaskExecution(ctx, task)
		require.NoError(t, err)

		// Add error
		taskError := TaskError{
			Message: "test error",
			Details: map[string]interface{}{"code": "TEST_ERROR"},
		}
		err = store.AddTaskError(ctx, "task1", taskError)
		assert.NoError(t, err)

		// Verify error added
		retrieved, err := store.GetTaskExecution(ctx, "task1")
		assert.NoError(t, err)
		assert.Len(t, retrieved.Errors, 1)
		assert.Equal(t, "test error", retrieved.Errors[0].Message)
	})

	t.Run("add error to non-existent task", func(t *testing.T) {
		err := store.AddTaskError(ctx, "nonexistent", TaskError{Message: "test"})
		assert.Error(t, err)
	})
}

func TestListTaskExecutions(t *testing.T) {
	store, mr := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()

	// Create test tasks
	tasks := []*TaskExecution{
		createTestTask("task1"),
		createTestTask("task2"),
		createTestTask("task3"),
	}
	tasks[1].Status = TaskStatusRunning
	tasks[2].Status = TaskStatusSuccess

	for _, task := range tasks {
		err := store.CreateTaskExecution(ctx, task)
		require.NoError(t, err)
	}

	t.Run("list by status", func(t *testing.T) {
		status := TaskStatusRunning
		filter := TaskFilter{
			Status: &status,
			Limit:  10,
		}

		listed, err := store.ListTaskExecutions(ctx, filter)
		assert.NoError(t, err)
		assert.Len(t, listed, 1)
		assert.Equal(t, TaskStatusRunning, listed[0].Status)
	})

	t.Run("list by time range", func(t *testing.T) {
		filter := TaskFilter{
			FromDate: time.Now().Add(-time.Hour),
			ToDate:   time.Now().Add(time.Hour),
			Limit:    10,
		}

		listed, err := store.ListTaskExecutions(ctx, filter)
		assert.NoError(t, err)
		assert.Len(t, listed, 3)
	})

	t.Run("list with limit", func(t *testing.T) {
		filter := TaskFilter{
			Limit: 2,
		}

		listed, err := store.ListTaskExecutions(ctx, filter)
		assert.NoError(t, err)
		assert.Len(t, listed, 2)
	})
}

func TestGetMostRecentTaskExecutions(t *testing.T) {
	store, mr := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()

	// Create test tasks with different timestamps
	for i := 0; i < 5; i++ {
		task := createTestTask(fmt.Sprintf("task%d", i))
		task.CreatedAt = time.Now().Add(time.Duration(-i) * time.Hour)
		err := store.CreateTaskExecution(ctx, task)
		require.NoError(t, err)
	}

	t.Run("get recent tasks", func(t *testing.T) {
		tasks, err := store.GetMostRecentTaskExecutions(ctx, 3)
		assert.NoError(t, err)
		assert.Len(t, tasks, 3)

		// Verify order (most recent first)
		for i := 1; i < len(tasks); i++ {
			assert.True(t, tasks[i-1].CreatedAt.After(tasks[i].CreatedAt))
		}
	})
}

func TestCleanupOldTaskExecutions(t *testing.T) {
	store, mr := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()

	// Create some old and new tasks
	oldTask := createTestTask("old_task")
	oldTask.CreatedAt = time.Now().Add(-24 * time.Hour)
	newTask := createTestTask("new_task")

	err := store.CreateTaskExecution(ctx, oldTask)
	require.NoError(t, err)
	err = store.CreateTaskExecution(ctx, newTask)
	require.NoError(t, err)

	t.Run("cleanup old tasks", func(t *testing.T) {
		err := store.CleanupOldTaskExecutions(ctx, 12*time.Hour)
		assert.NoError(t, err)

		// Verify old task removed
		_, err = store.GetTaskExecution(ctx, "old_task")
		assert.Error(t, err)

		// Verify new task remains
		task, err := store.GetTaskExecution(ctx, "new_task")
		assert.NoError(t, err)
		assert.NotNil(t, task)

		// Verify timeline and status sets updated
		members, err := store.client.ZRange(ctx, timelineKey, 0, -1).Result()
		assert.NoError(t, err)
		assert.NotContains(t, members, "old_task")
		assert.Contains(t, members, "new_task")
	})
}

func TestConcurrentOperations(t *testing.T) {
	store, mr := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()
	const numGoroutines = 10

	t.Run("concurrent creates", func(t *testing.T) {
		var wg sync.WaitGroup
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Add(-1)
				task := createTestTask(fmt.Sprintf("concurrent_task_%d", i))
				err := store.CreateTaskExecution(ctx, task)
				assert.NoError(t, err)
			}(i)
		}
		wg.Wait()

		// Verify all tasks created
		for i := 0; i < numGoroutines; i++ {
			taskID := fmt.Sprintf("concurrent_task_%d", i)
			task, err := store.GetTaskExecution(ctx, taskID)
			assert.NoError(t, err)
			assert.NotNil(t, task)
		}
	})

	t.Run("concurrent updates", func(t *testing.T) {
		task := createTestTask("update_task")
		err := store.CreateTaskExecution(ctx, task)
		require.NoError(t, err)

		var wg sync.WaitGroup
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Add(-1)
				err := store.UpdateTaskStatus(ctx, "update_task", TaskStatusRunning)
				assert.NoError(t, err)
			}()
		}
		wg.Wait()

		// Verify final state
		task, err = store.GetTaskExecution(ctx, "update_task")
		assert.NoError(t, err)
		assert.Equal(t, TaskStatusRunning, task.Status)
	})
}

func TestDeleteTaskExecution(t *testing.T) {
	store, mr := setupTestRedis(t)
	defer mr.Close()

	ctx := context.Background()

	t.Run("delete existing task", func(t *testing.T) {
		// Create a task first
		task := createTestTask("task_to_delete")
		err := store.CreateTaskExecution(ctx, task)
		require.NoError(t, err)

		// Delete the task
		err = store.DeleteTaskExecution(ctx, "task_to_delete")
		assert.NoError(t, err)

		// Verify task is deleted
		_, err = store.GetTaskExecution(ctx, "task_to_delete")
		assert.Error(t, err)

		// Verify task removed from timeline
		members, err := store.client.ZRange(ctx, timelineKey, 0, -1).Result()
		assert.NoError(t, err)
		assert.NotContains(t, members, "task_to_delete")

		// Verify task removed from status set
		statusKey := statusPrefix + string(TaskStatusPending)
		members, err = store.client.ZRange(ctx, statusKey, 0, -1).Result()
		assert.NoError(t, err)
		assert.NotContains(t, members, "task_to_delete")

		// Verify hash is deleted
		exists, err := store.client.Exists(ctx, taskPrefix+"task_to_delete").Result()
		assert.NoError(t, err)
		assert.Equal(t, int64(0), exists)
	})

	t.Run("delete non-existent task", func(t *testing.T) {
		err := store.DeleteTaskExecution(ctx, "nonexistent_task")
		assert.Error(t, err)
	})

	t.Run("delete task with different statuses", func(t *testing.T) {
		// Test deletion of tasks in different states to ensure proper cleanup
		statuses := []TaskStatus{
			TaskStatusPending,
			TaskStatusRunning,
			TaskStatusSuccess,
			TaskStatusFailed,
		}

		for _, status := range statuses {
			taskID := fmt.Sprintf("task_%s", status)
			task := createTestTask(taskID)
			task.Status = status

			err := store.CreateTaskExecution(ctx, task)
			require.NoError(t, err)

			err = store.DeleteTaskExecution(ctx, taskID)
			assert.NoError(t, err)

			// Verify deletion
			_, err = store.GetTaskExecution(ctx, taskID)
			assert.Error(t, err)

			// Verify removed from status set
			statusKey := statusPrefix + string(status)
			members, err := store.client.ZRange(ctx, statusKey, 0, -1).Result()
			assert.NoError(t, err)
			assert.NotContains(t, members, taskID)
		}
	})

	t.Run("concurrent deletes", func(t *testing.T) {
		// Create tasks
		numTasks := 10
		var wg sync.WaitGroup

		// Create tasks first
		for i := 0; i < numTasks; i++ {
			taskID := fmt.Sprintf("concurrent_delete_task_%d", i)
			task := createTestTask(taskID)
			err := store.CreateTaskExecution(ctx, task)
			require.NoError(t, err)
		}

		// Concurrently delete tasks
		for i := 0; i < numTasks; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				taskID := fmt.Sprintf("concurrent_delete_task_%d", i)
				err := store.DeleteTaskExecution(ctx, taskID)
				assert.NoError(t, err)
			}(i)
		}
		wg.Wait()

		// Verify all tasks are deleted
		for i := 0; i < numTasks; i++ {
			taskID := fmt.Sprintf("concurrent_delete_task_%d", i)
			_, err := store.GetTaskExecution(ctx, taskID)
			assert.Error(t, err)
		}

		// Verify timeline is empty
		count, err := store.client.ZCard(ctx, timelineKey).Result()
		assert.NoError(t, err)
		assert.Equal(t, int64(0), count)
	})

	t.Run("delete and recreate task", func(t *testing.T) {
		taskID := "recreate_task"

		// Create initial task
		task := createTestTask(taskID)
		err := store.CreateTaskExecution(ctx, task)
		require.NoError(t, err)

		// Delete task
		err = store.DeleteTaskExecution(ctx, taskID)
		assert.NoError(t, err)

		// Recreate task
		newTask := createTestTask(taskID)
		newTask.Args = map[string]interface{}{"new": "value"}
		err = store.CreateTaskExecution(ctx, newTask)
		assert.NoError(t, err)

		// Verify new task exists with new values
		retrieved, err := store.GetTaskExecution(ctx, taskID)
		assert.NoError(t, err)
		assert.Equal(t, map[string]interface{}{"new": "value"}, retrieved.Args)
	})
}
