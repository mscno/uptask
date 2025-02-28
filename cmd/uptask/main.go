package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/goccy/go-yaml"
	"github.com/google/uuid"
	"github.com/mscno/uptask/internal/events"
	"github.com/samber/lo"
	"log/slog"
	"net/http"
	"os"
	"time"
)

type QJob struct {
	Kind               string                 `yaml:"kind"`
	Destination        string                 `yaml:"destination"`
	Method             string                 `yaml:"method"`
	Cron               string                 `yaml:"cron"`
	Enabled            bool                   `yaml:"enabled"`
	Payload            map[string]interface{} `yaml:"args"`
	Headers            map[string]string      `yaml:"headers"`
	ContentType        string                 `yaml:"content_type"`
	Retries            int                    `yaml:"retries"`
	Timeout            string                 `yaml:"timeout,omitempty"`
	CallbackURL        string                 `yaml:"callback,omitempty"`
	FailureCallbackURL string                 `yaml:"failureCallback,omitempty"`
}

type QJobsConfig struct {
	QJobs []QJob `json:"qjobs"`
}

func main() {
	err := process()
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func process() error {
	fmt.Println("applying qjobs...")
	var debug bool
	flag.BoolVar(&debug, "debug", false, "enable debug logging")
	flag.Parse()
	if debug {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}
	ctx := context.Background()
	if len(os.Args) < 2 {
		return errors.New("Usage: qjobs <file>")
	}

	qstashToken, ok := os.LookupEnv("QSTASH_TOKEN")
	if !ok {
		return errors.New("QSTASH_TOKEN not set")
	}

	slog.Debug("reading qjobs file", "qjobs_file", os.Args[1])

	ymlBytes, err := os.ReadFile(os.Args[1])
	if err != nil {
		return err
	}

	var v QJobsConfig

	if err := yaml.Unmarshal(ymlBytes, &v); err != nil {
		return err
	}

	var jobNames []string
	//for _, job := range v.QJobs {
	//	jobNames = append(jobNames, job.ScheduleId)
	//}

	scheduledJobs, err := listQstashJobs(ctx, qstashToken)
	if err != nil {
		return err
	}

	slog.Debug("fetched scheduled jobs", "scheduled_jobs", len(scheduledJobs))

	schedulesToDelete := []string{}
	for _, job := range scheduledJobs {
		if !lo.Contains(jobNames, job.ScheduleId) {
			schedulesToDelete = append(schedulesToDelete, job.ScheduleId)
		}
	}

	for _, scheduleID := range schedulesToDelete {
		slog.Debug("deleting schedule", "schedule_id", scheduleID)
		if err := deleteQstashJob(ctx, qstashToken, scheduleID); err != nil {
			return err
		}
		slog.Debug("deleted schedule successfully", "schedule_id", scheduleID)
	}

	for _, job := range v.QJobs {
		slog.Debug("creating schedule", "destination", job.Destination)
		res, err := createQstashJob(ctx, qstashToken, job)
		if err != nil {
			return err
		}
		slog.Debug("created schedule successfully", "job_name", res.ScheduleId)
	}

	return nil

}

// Define QstashScheduledJob struct with only necessary fields
// QstashScheduledJob represents the scheduling configuration for a QStash job
type QstashScheduledJob struct {
	ScheduleId  string              `json:"scheduleId"`
	CreatedAt   int64               `json:"createdAt,omitempty"`
	Cron        string              `json:"cron"`
	Destination string              `json:"destination"`
	Method      string              `json:"method,omitempty"`
	Body        string              `json:"body,omitempty"`
	Header      map[string][]string `json:"header,omitempty"`
	Retries     int                 `json:"retries,omitempty"`
}

func setJobDefaults(job QJob) QJob {

	if job.Method == "" {
		job.Method = "POST"
	}

	if job.Timeout == "" {
		job.Timeout = "30s"
	}

	if job.Retries == 0 {
		job.Retries = 3
	}

	if job.ContentType == "" {
		job.ContentType = "application/json"
	}

	return job

}

// URL for QStash scheduled jobs
const listScheduledURL = "https://qstash.upstash.io/v2/schedules"

// listQstashJobs fetches scheduled jobs from QStash
func listQstashJobs(ctx context.Context, qstashToken string) ([]QstashScheduledJob, error) {
	slog.Debug("fetching scheduled jobs from qstash")
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	// Prepare request
	req, err := http.NewRequestWithContext(ctx, "GET", listScheduledURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+qstashToken)

	// Send request
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("failed to list scheduled jobs: non-200 response code")
	}

	// Parse response body
	var jobs []QstashScheduledJob
	if err := json.NewDecoder(resp.Body).Decode(&jobs); err != nil {
		return nil, fmt.Errorf("failed to decode listQstashJobs response: %w", err)
	}

	return jobs, nil
}

// deleteQstashJob deletes a scheduled job from QStash
func deleteQstashJob(ctx context.Context, qstashToken, scheduleID string) error {
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	// Construct the delete URL using the scheduleID
	deleteURL := fmt.Sprintf("https://qstash.upstash.io/v2/schedules/%s", scheduleID)

	// Prepare request
	req, err := http.NewRequestWithContext(ctx, "DELETE", deleteURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+qstashToken)

	// Send request
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to delete scheduled job: non-200 response code %d", resp.StatusCode)
	}

	return nil
}

type task struct {
	kind    string
	payload map[string]interface{}
}

func (t task) Kind() string {
	return t.kind
}

func (t task) Payload() any {
	return t.payload
}

// createQstashJob schedules a new job on QStash using a QstashScheduledJob struct
func createQstashJob(ctx context.Context, qstashToken string, job QJob) (*QstashScheduledJob, error) {
	job = setJobDefaults(job)
	client := &http.Client{
		Timeout: 35 * time.Second,
	}

	// Construct the create URL
	createURL := "https://qstash.upstash.io/v2/schedules"

	var buffer bytes.Buffer
	switch job.ContentType {
	case "application/json":
		if err := json.NewEncoder(&buffer).Encode(job.Payload); err != nil {
			return nil, fmt.Errorf("failed to encode payload: %w", err)
		}
	case "text/plain":
		buffer.WriteString(fmt.Sprintf("%v", job.Payload))
	case "application/cloudevents+json":
		t := task{
			kind:    job.Kind,
			payload: job.Payload,
		}

		ce, err := events.Serialize(ctx, t)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize payload: %w", err)
		}
		ce.SetID(uuid.Nil.String())
		payload, err := ce.MarshalJSON()
		if err != nil {
			return nil, fmt.Errorf("failed to encode payload: %w", err)
		}
		fmt.Println(string(payload))
		buffer.Write(payload)
	default:
		return nil, fmt.Errorf("unsupported content type: %s", job.ContentType)
	}

	// Prepare request with body
	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/%s", createURL, job.Destination), &buffer)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	req.Header.Set("Authorization", "Bearer "+qstashToken)
	req.Header.Set("Upstash-Cron", job.Cron)
	req.Header.Set("Upstash-Destination", job.Destination)
	if job.ContentType != "" {
		req.Header.Set("Content-Type", job.ContentType)
	}
	for k, v := range job.Headers {
		req.Header.Set(fmt.Sprintf("Upstash-Forward-%s", k), v)
	}
	if job.Method != "" {
		req.Header.Set("Upstash-Method", job.Method)
	}
	if job.Timeout != "" {
		req.Header.Set("Upstash-Timeout", job.Timeout)
	}
	if job.Retries > 0 {
		req.Header.Set("Upstash-Retries", fmt.Sprintf("%d", job.Retries))
		req.Header.Set("Upstash-Forward-Upstash-Retries", fmt.Sprintf("%d", job.Retries))
	}
	if job.CallbackURL != "" {
		req.Header.Set("Upstash-Callback", job.CallbackURL)
	}
	if job.FailureCallbackURL != "" {
		req.Header.Set("Upstash-Failure-Callback", job.FailureCallbackURL)
	}

	// Send request
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body := bytes.NewBuffer(nil)
		body.ReadFrom(resp.Body)
		slog.Error("failed to create scheduled job", "status_code", resp.StatusCode, "body", body.String())
		return nil, fmt.Errorf("failed to create scheduled job: non-200/201 response code %d", resp.StatusCode)
	}

	// Parse response body
	var createdJob QstashScheduledJob
	if err := json.NewDecoder(resp.Body).Decode(&createdJob); err != nil {
		return nil, fmt.Errorf("failed to decode createQstashJob response: %w", err)
	}

	return &createdJob, nil
}
