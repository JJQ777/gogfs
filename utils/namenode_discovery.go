package utils

import (
	"bufio"
	"context"
	"log"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NamenodeClusterConfig holds the cluster configuration
type NamenodeClusterConfig struct {
	Namenodes       []string
	Primary         string
	FailoverTimeout int
	RetryInterval   int
}

// LoadClusterConfig loads namenode cluster configuration from file
func LoadClusterConfig(configPath string) (*NamenodeClusterConfig, error) {
	config := &NamenodeClusterConfig{
		FailoverTimeout: 5,
		RetryInterval:   2,
	}

	file, err := os.Open(configPath)
	if err != nil {
		// Return default config if file doesn't exist
		log.Printf("⚠️  Config file not found, using defaults")
		config.Namenodes = []string{"localhost:8080", "localhost:8081"}
		config.Primary = "localhost:8080"
		return config, nil
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		switch key {
		case "namenodes":
			config.Namenodes = strings.Split(value, ",")
			for i := range config.Namenodes {
				config.Namenodes[i] = strings.TrimSpace(config.Namenodes[i])
			}
		case "primary":
			config.Primary = value
		}
	}

	if len(config.Namenodes) == 0 {
		config.Namenodes = []string{"localhost:8080"}
	}
	if config.Primary == "" && len(config.Namenodes) > 0 {
		config.Primary = config.Namenodes[0]
	}

	return config, nil
}

// DiscoverActiveNamenode tries to connect to available namenodes and returns an active connection
func DiscoverActiveNamenode(config *NamenodeClusterConfig) (*grpc.ClientConn, string, error) {
	// Try primary first
	if config.Primary != "" {
		if conn, err := tryConnect(config.Primary); err == nil {
			log.Printf("✅ Connected to primary NameNode at %s", config.Primary)
			return conn, config.Primary, nil
		}
		log.Printf("⚠️  Primary NameNode %s unavailable, trying alternates...", config.Primary)
	}

	// Try all namenodes
	for _, nn := range config.Namenodes {
		if nn == config.Primary {
			continue // Already tried
		}
		if conn, err := tryConnect(nn); err == nil {
			log.Printf("✅ Connected to NameNode at %s", nn)
			return conn, nn, nil
		}
	}

	return nil, "", &ConnectionError{Message: "No available NameNode found"}
}

// tryConnect attempts to establish a gRPC connection with timeout
func tryConnect(address string) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// ConnectionError represents a connection failure
type ConnectionError struct {
	Message string
}

func (e *ConnectionError) Error() string {
	return e.Message
}
