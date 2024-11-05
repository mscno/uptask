package testutl

import (
	"bufio"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func GetPort() int {
	min := 1400
	max := 7000
	for {
		port := rand.Intn(max-min) + min
		lis, err := net.Listen("tcp", ":"+strconv.Itoa(port))
		if err == nil {
			err := lis.Close()
			if err != nil {
				slog.Error(err.Error())
			}
			time.Sleep(time.Millisecond * 50)
			return port
		}
	}
}

func StartLocalTunnel(port int) (string, *exec.Cmd, error) {
	// Start the command with the specified port
	cmd := exec.Command("npx", "localtunnel", "--port", fmt.Sprintf("%d", port))

	// Get the stdout pipe to capture the output
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", nil, err
	}

	// Start the command asynchronously
	if err := cmd.Start(); err != nil {
		return "", nil, err
	}

	// Create a scanner to read the command's output line by line
	scanner := bufio.NewScanner(stdout)
	re := regexp.MustCompile(`your url is:\s*(https://[^\s]+)`)

	var tunnelURL string

	// Scan the output for the URL
	for scanner.Scan() {
		line := scanner.Text()
		fmt.Println(line) // Optional: For debugging purposes

		// Check if the line contains the URL
		matches := re.FindStringSubmatch(line)
		if len(matches) > 1 {
			tunnelURL = matches[1]
			break
		}
	}

	// Check for scanning errors
	if err := scanner.Err(); err != nil {
		return "", nil, err
	}

	// If no URL was found, return an error
	if tunnelURL == "" {
		return "", nil, fmt.Errorf("failed to find tunnel URL")
	}

	// Return the tunnel URL, the command handle, and no error
	return tunnelURL, cmd, nil
}

func ReadTokenFromEnv() string {
	if token := os.Getenv("QSTASH_TOKEN"); token != "" {
		return token
	}

	if ok, path := findConfigFile(".env"); ok {
		content, err := os.ReadFile(path)
		if err != nil {
			panic(err)
		}
		r := strings.NewReader(string(content))
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "QSTASH_TOKEN=") {
				v := strings.TrimPrefix(line, "QSTASH_TOKEN=")
				v = strings.TrimSpace(v)
				v = strings.Trim(v, "\"")
				return v
			}
		}
	}
	panic("QSTASH_TOKEN not found")
}

func repeatString(s string, count int) string {
	var result string
	for i := 0; i < count; i++ {
		result += s
	}
	return result
}

func findConfigFile(fileName string) (bool, string) {
	const maxAttempts = 10

	for i := 0; i < maxAttempts; i++ {
		path := filepath.Join(repeatString("../", i), fileName)
		// Check if we have a go.mod file in the directory to stop the search as we have reached the root
		if _, err := os.Stat(path); err == nil {
			return true, path
		}
		if _, err := os.Stat(filepath.Join(repeatString("../", i), "go.mod")); err == nil {
			break
		}
	}
	return false, ""
}
