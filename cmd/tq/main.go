package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/mscno/uptask"
	"os"
	"time"
)

type model struct {
	width       int
	height      int
	tableHeight int
	lastUpdate  time.Time

	baseStyle  lipgloss.Style
	viewStyle  lipgloss.Style
	tasksTable table.Model

	tab        int
	tabs       []uptask.TaskStatus
	tableStyle table.Styles

	tasks        []*uptask.TaskExecution
	activeTask   *uptask.TaskExecution
	filterStatus *uptask.TaskStatus
	taskStore    *uptask.RedisTaskStore

	err error
}

var (
	tabStyle        = lipgloss.NewStyle().Padding(0, 1).Foreground(Color.Secondary)
	activeTabStyle  = lipgloss.NewStyle().Padding(0, 1).Bold(true).Foreground(Color.Highlight)
	headerStyle     = lipgloss.NewStyle().Bold(true).Foreground(Color.Primary).BorderBottom(true).PaddingBottom(1)
	subtleTextStyle = lipgloss.NewStyle().Foreground(Color.Secondary)
)

type Theme struct {
	Primary   lipgloss.AdaptiveColor
	Secondary lipgloss.AdaptiveColor
	Highlight lipgloss.AdaptiveColor
	Border    lipgloss.AdaptiveColor
	Green     lipgloss.AdaptiveColor
	Red       lipgloss.AdaptiveColor
}

var Color = Theme{
	Primary:   lipgloss.AdaptiveColor{Light: "#000000", Dark: "#FFFFFF"},
	Secondary: lipgloss.AdaptiveColor{Light: "#969B86", Dark: "#696969"},
	Highlight: lipgloss.AdaptiveColor{Light: "#8b2def", Dark: "#8b2def"},
	Border:    lipgloss.AdaptiveColor{Light: "#D9DCCF", Dark: "#383838"},
	Green:     lipgloss.AdaptiveColor{Light: "#00FF00", Dark: "#00FF00"},
	Red:       lipgloss.AdaptiveColor{Light: "#FF0000", Dark: "#FF0000"},
}

type FetchMsg struct{}

var _ tea.Model = (*model)(nil)

func main() {

	m := initializeModel()
	p := tea.NewProgram(m)
	if err := p.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "could not start program: %s\n", err)
		os.Exit(1)
	}
}

func initializeModel() *model {
	qstashToken := os.Getenv("QSTASH_TOKEN")
	if qstashToken == "" {
		fmt.Println("QSTASH_TOKEN environment variable is required")
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
	store, err := uptask.NewRedisTaskStore(uptask.RedisConfig{
		Addr:     fmt.Sprintf("%s:%s", redisUrl, redisPort),
		Username: "default",
		Password: redisPassword,
	})
	if err != nil {
		fmt.Println("Failed to create task store", err)
		os.Exit(1)
	}
	// Initialize uptask store and check environment variables
	// ... (similar setup as you posted)
	// Initialize Bubble Tea model with tabs and initial tasks

	tabs := []uptask.TaskStatus{
		"ALL",
		uptask.TaskStatusPending,
		uptask.TaskStatusRunning,
		uptask.TaskStatusSuccess,
		uptask.TaskStatusFailed,
	}

	tableStyle := table.DefaultStyles()
	tableStyle.Selected = lipgloss.NewStyle().Background(Color.Highlight)
	tableHeight := 20
	// Create a table model for task display
	t := table.New(
		table.WithColumns([]table.Column{
			{Title: "ID", Width: 36},
			{Title: "Task Kind", Width: 25},
			{Title: "Status", Width: 12},
			{Title: "Attempt", Width: 8},
			{Title: "Queue", Width: 10},
			{Title: "Created At", Width: 35},
			{Title: "Scheduled At", Width: 35},
		}),
		table.WithStyles(tableStyle),
		table.WithFocused(true),
		table.WithHeight(tableHeight),
		table.WithRows([]table.Row{}),
	)

	return &model{
		tableStyle: tableStyle,
		//baseStyle:    lipgloss.NewStyle().Margin(1, 0, 1, 0),
		tabs:         tabs,
		tab:          0,
		tableHeight:  tableHeight,
		filterStatus: &tabs[0],
		tasksTable:   t,
		taskStore:    store, /* Initialize uptask store */
	}
}

// Fetch tasks and update the table data
func (m *model) fetchTasks() {
	ctx := context.Background()
	filterStatus := m.filterStatus
	if *filterStatus == "ALL" {
		filterStatus = nil
	}
	tasks, err := m.taskStore.ListTaskExecutions(ctx, uptask.TaskFilter{
		Status: filterStatus,
		Limit:  100,
	})
	if err != nil {
		m.err = err
		return
	}

	m.tasks = tasks
	m.updateTable()
}

func (m *model) setActiveTask(taskID string) {
	ctx := context.Background()
	task, err := m.taskStore.GetTaskExecution(ctx, taskID)
	if err != nil {
		m.err = err
		return
	}
	m.activeTask = task
}

// Update table rows based on filtered tasks
func (m *model) updateTable() {
	rows := make([]table.Row, len(m.tasks))
	for i, task := range m.tasks {
		status := renderStatust(task.Status) // Only this cell will have colored text
		rows[i] = table.Row{
			task.ID,
			task.TaskKind,
			status,
			fmt.Sprintf("%d/%d", task.Attempt, task.MaxAttempts),
			task.Queue,
			dashIfZeroTimeAgo(task.CreatedAt),
			dashIfZeroTimeScheduled(task.ScheduledAt),
		}
	}
	m.lastUpdate = time.Now()
	m.tasksTable.SetRows(rows)
}

// Update and Render function
func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.height = msg.Height
		m.width = msg.Width
		//m.tableHeight = m.height - 5
		//m.tasksTable.SetHeight(m.tableHeight)
	case FetchMsg:
		m.fetchTasks()
	case tea.KeyMsg:
		switch msg.String() {
		case "esc", "backspace":
			if m.activeTask != nil {
				m.activeTask = nil
				break
			}
			if m.tasksTable.Focused() {
				m.tableStyle.Selected = m.baseStyle
				m.tasksTable.SetStyles(m.tableStyle)
				m.tasksTable.Blur()
			} else {
				m.tableStyle.Selected = m.tableStyle.Selected.Background(Color.Highlight)
				m.tasksTable.SetStyles(m.tableStyle)
				m.tasksTable.Focus()
			}
			// Moves the focus up in the process table if the table is focused.
		case "up", "k":
			if m.tasksTable.Focused() {
				m.tasksTable.MoveUp(1)
			}
		// Moves the focus down in the process table if the table is focused.
		case "down", "j":
			if m.tasksTable.Focused() {
				m.tasksTable.MoveDown(1)
			}
		case "pgup":
			if m.tasksTable.Focused() {
				m.tasksTable.MoveUp(m.tableHeight - 1)
			}
		case "pgdown":
			if m.tasksTable.Focused() {
				m.tasksTable.MoveDown(m.tableHeight - 1)
			}
		case "tab": // Navigate to the next tab
			m.tab = (m.tab + 1) % len(m.tabs)
			m.filterStatus = &m.tabs[m.tab]
			m.fetchTasks() // Fetch tasks for the new status
		case "q", "ctrl+c": // Quit
			return m, tea.Quit
		case "enter":
			if m.tasksTable.Focused() {
				// Get the selected task ID
				selectedRow := m.tasksTable.SelectedRow()
				if selectedRow == nil {
					break
				}
				taskID := selectedRow[0]
				m.setActiveTask(taskID)
			}
		case "u": // Fetch tasks
			m.fetchTasks()
		}
	case tea.QuitMsg:
		return m, tea.Quit
	}
	return m, nil
}

func (m *model) Init() tea.Cmd {
	return func() tea.Msg {
		return FetchMsg{}
	}
}

// Enhance the View function to include the viewHeader
func (m model) View() string {
	if m.err != nil {
		return fmt.Sprintf("Error: %v\n", m.err)
	}
	column := m.baseStyle.Width(m.width).Padding(1, 0, 0, 0).Render
	var content string
	if m.activeTask != nil {
		content = m.viewTask()
	} else {
		content = m.baseStyle.
			Width(m.width).
			Height(m.height).
			Render(
				// Vertically join multiple elements aligned to the left.
				lipgloss.JoinVertical(lipgloss.Left,
					column(m.viewHeader()),
					column(m.viewStyle.Render(m.tasksTable.View())),
				),
			)
	}

	return content
}

// Uses lipgloss.JoinVertical and lipgloss.JoinHorizontal to arrange the header content.
// It displays the last update time and various system statistics (CPU and memory usage) in a structured format.
// Create a viewHeader function that displays tabs and metadata
func (m model) viewHeader() string {
	var tabs []string
	for i, status := range m.tabs {
		if i == m.tab {
			tabs = append(tabs, activeTabStyle.Render(string(status)))
		} else {
			tabs = append(tabs, tabStyle.Render(string(status)))
		}
	}

	// Join the tabs horizontally
	tabsRow := lipgloss.JoinHorizontal(lipgloss.Top, tabs...)

	// Add a last updated time display
	lastUpdated := subtleTextStyle.Render(fmt.Sprintf("Last updated: %s", m.lastUpdate.Format("15:04:05")))
	if m.lastUpdate.IsZero() {
		lastUpdated = subtleTextStyle.Render("Last updated: N/A")
	}
	// Return the combined header with tabs and the last updated time
	return lipgloss.JoinVertical(lipgloss.Top,
		headerStyle.Render("Task Viewer"), // Header Title
		tabsRow,                           // Rendered tabs
		lastUpdated,                       // Metadata row
	)
}

func (m model) viewTask() string {
	if m.activeTask == nil {
		return ""
	}

	// Styling for labels and right-aligned, fixed-width values
	labelStyle := lipgloss.NewStyle().Bold(true).Foreground(Color.Secondary).Width(20).Align(lipgloss.Right).Padding(0, 1)
	valueStyle := lipgloss.NewStyle().Foreground(Color.Primary).Align(lipgloss.Left)

	// Format each property of the task with right-aligned values
	id := lipgloss.JoinHorizontal(lipgloss.Left, labelStyle.Render("Task ID:"), valueStyle.Render(m.activeTask.ID))
	taskKind := lipgloss.JoinHorizontal(lipgloss.Left, labelStyle.Render("Task Kind:"), valueStyle.Render(m.activeTask.TaskKind))
	status := lipgloss.JoinHorizontal(lipgloss.Left, labelStyle.Render("Status:"), valueStyle.Render(renderStatus(m.activeTask.Status)))
	attempts := lipgloss.JoinHorizontal(lipgloss.Left, labelStyle.Render("Attempts:"), valueStyle.Render(fmt.Sprintf("%d/%d", m.activeTask.Attempt, m.activeTask.MaxAttempts)))
	queue := lipgloss.JoinHorizontal(lipgloss.Left, labelStyle.Render("Queue:"), valueStyle.Render(m.activeTask.Queue))
	createdAt := lipgloss.JoinHorizontal(lipgloss.Left, labelStyle.Render("Created At:"), valueStyle.Render(dashIfZeroTimeAgo(m.activeTask.CreatedAt)))
	attemptedAt := lipgloss.JoinHorizontal(lipgloss.Left, labelStyle.Render("Attempted At:"), valueStyle.Render(dashIfZeroTimeAgo(m.activeTask.AttemptedAt)))
	scheduledAt := lipgloss.JoinHorizontal(lipgloss.Left, labelStyle.Render("Scheduled At:"), valueStyle.Render(dashIfZeroTimeScheduled(m.activeTask.ScheduledAt)))
	finalizedAt := lipgloss.JoinHorizontal(lipgloss.Left, labelStyle.Render("Finalized At:"), valueStyle.Render(dashIfZeroTimeAgo(m.activeTask.FinalizedAt)))
	qstashMessageID := lipgloss.JoinHorizontal(lipgloss.Left, labelStyle.Render("QStash Message ID:"), valueStyle.Render(m.activeTask.QstashMessageID))
	scheduleID := lipgloss.JoinHorizontal(lipgloss.Left, labelStyle.Render("Schedule ID:"), valueStyle.Render(m.activeTask.ScheduleID))

	// Format Args as pretty-printed JSON
	var args string
	if m.activeTask.Args != nil {
		argsJSON, err := json.MarshalIndent(m.activeTask.Args, "", "  ")
		if err == nil {
			args = lipgloss.JoinHorizontal(lipgloss.Left, labelStyle.Render("Args:"), lipgloss.NewStyle().Foreground(Color.Primary).Render(string(argsJSON)))
		} else {
			args = lipgloss.JoinHorizontal(lipgloss.Left, labelStyle.Render("Args:"), valueStyle.Render("Error formatting args"))
		}
	} else {
		args = lipgloss.JoinHorizontal(lipgloss.Left, labelStyle.Render("Args:"), valueStyle.Render("None"))
	}

	// Display errors if there are any
	var errors string
	if len(m.activeTask.Errors) > 0 {
		errorsList := []string{labelStyle.Render("Errors:")}
		for _, err := range m.activeTask.Errors {
			errorMsg := fmt.Sprintf("- %s (at %s)", err.Message, err.Timestamp.Format("2006-01-02 15:04:05"))
			errorsList = append(errorsList, valueStyle.Render(errorMsg))
		}
		errors = lipgloss.JoinVertical(lipgloss.Left, errorsList...)
	} else {
		errors = lipgloss.JoinHorizontal(lipgloss.Left, labelStyle.Render("Errors:"), valueStyle.Render("None"))
	}

	// Join all parts of the task details into a vertical layout
	return lipgloss.JoinVertical(lipgloss.Left,
		headerStyle.Render("Task Details"), // Header
		taskKind,
		id,
		status,
		attempts,
		queue,
		createdAt,
		attemptedAt,
		scheduledAt,
		finalizedAt,
		qstashMessageID,
		scheduleID,
		args, // Display formatted Args
		errors,
	)
}

func renderStatus(status uptask.TaskStatus) string {
	switch status {
	case uptask.TaskStatusPending:
		return lipgloss.NewStyle().Foreground(Color.Secondary).Render(string(status))
	case uptask.TaskStatusRunning:
		return lipgloss.NewStyle().Foreground(Color.Primary).Render(string(status))
	case uptask.TaskStatusSuccess:
		return lipgloss.NewStyle().Foreground(Color.Green).Render(string(status))
	case uptask.TaskStatusFailed:
		return lipgloss.NewStyle().Foreground(Color.Red).Render(string(status))
	default:
		return string(status)
	}
}
func renderStatust(status uptask.TaskStatus) string {
	switch status {
	case uptask.TaskStatusPending:
		return fmt.Sprintf("⏳  %s", status) // Symbol for pending
	case uptask.TaskStatusRunning:
		return fmt.Sprintf("🔄 %s", status) // Symbol for running
	case uptask.TaskStatusSuccess:
		return fmt.Sprintf("✅  %s", status) // Symbol for success
	case uptask.TaskStatusFailed:
		return fmt.Sprintf("❌  %s", status) // Symbol for failed
	default:
		return string(status)
	}
}

func dashIfZeroTimeAgo(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	now := time.Now()
	ago := now.Sub(t)
	var agoStr string
	if ago < time.Minute {
		agoStr = fmt.Sprintf("%ds ago", int(ago.Seconds()))
	} else if ago < time.Hour {
		agoStr = fmt.Sprintf("%dm ago", int(ago.Minutes()))
	} else if ago < 24*time.Hour {
		agoStr = fmt.Sprintf("%dh ago", int(ago.Hours()))
	} else {
		agoStr = fmt.Sprintf("%dd ago", int(ago.Hours()/24))
	}
	return t.Format("2006-01-02 15:04:05") + " (" + agoStr + ")"
}

func dashIfZeroTimeScheduled(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	now := time.Now()
	in := t.Sub(now)
	var inStr string
	if in < time.Minute {
		inStr = fmt.Sprintf("in %ds", int(in.Seconds()))
	} else if in < time.Hour {
		inStr = fmt.Sprintf("in %dm", int(in.Minutes()))
	} else if in < 24*time.Hour {
		inStr = fmt.Sprintf("in %dh", int(in.Hours()))
	} else {
		inStr = fmt.Sprintf("in %dd", int(in.Hours()/24))
	}
	return t.Format("2006-01-02 15:04:05") + " (" + inStr + ")"
}

func addInXMinutes(t time.Time, minutes int) time.Time {

	return t.Add(time.Duration(minutes) * time.Minute)
}
