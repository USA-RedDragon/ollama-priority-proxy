// Package config holds the proxy's configuration.
package config

//go:generate go tool configulator -type Config

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// LogLevel is the minimum level of log messages that are written.
type LogLevel string

// Log levels accepted in log-level.
const (
	LogLevelDebug LogLevel = "debug"
	LogLevelInfo  LogLevel = "info"
	LogLevelWarn  LogLevel = "warn"
	LogLevelError LogLevel = "error"
)

// Config is the proxy's configuration.
type Config struct {
	LogLevel        LogLevel `name:"log-level" default:"info" description:"Logging level. One of debug, info, warn or error"`
	HTTP            HTTP     `name:"http" description:"Proxy listener"`
	Metrics         Metrics  `name:"metrics" description:"Prometheus metrics listener"`
	Upstream        Upstream `name:"upstream" description:"Ollama server"`
	Low             Low      `name:"low" description:"Scheduling of low priority requests"`
	GenerationPaths []string `name:"generation-paths" default:"/v1/chat/completions,/v1/completions,/v1/embeddings,/v1/responses,/api/chat,/api/generate,/api/embed,/api/embeddings" description:"Upstream paths whose POST requests are scheduled by priority. Other requests go straight to the upstream"`
}

// HTTP configures the proxy listener.
type HTTP struct {
	Bind string `name:"bind" default:"[::]" description:"Address to listen on. The default, [::], listens on all interfaces"`
	Port uint16 `name:"port" default:"11434" description:"Port to serve the proxy, /health and /ready on"`
}

// Metrics configures the Prometheus metrics listener.
type Metrics struct {
	Enabled bool   `name:"enabled" default:"true" description:"Serve Prometheus metrics on /metrics"`
	Bind    string `name:"bind" default:"[::]" description:"Address for the metrics listener"`
	Port    uint16 `name:"port" default:"9090" description:"Port for the metrics listener"`
}

// Upstream configures the Ollama server that requests are forwarded to.
type Upstream struct {
	URL string `name:"url" default:"http://127.0.0.1:11434" description:"Base URL of the Ollama server"`
}

// Low configures how low priority requests are scheduled.
type Low struct {
	MaxInFlight uint          `name:"max-in-flight" default:"1" description:"Most low priority generation requests sent to the upstream at once"`
	IdleGrace   time.Duration `name:"idle-grace" default:"2s" description:"How long no high priority generation request must be in flight before a low priority one is sent, so the steps of an agent's turn are not split by background work. Longer values delay background work more; 0 disables"`
	MaxWait     time.Duration `name:"max-wait" default:"0s" description:"How long a low priority request may wait in the queue before it fails with 503 and Retry-After; 0 waits forever"`
}

var (
	ErrInvalidLogLevel       = errors.New("log-level must be one of debug, info, warn or error")
	ErrInvalidUpstreamURL    = errors.New("upstream.url must be an absolute http or https URL")
	ErrInvalidMaxInFlight    = errors.New("low.max-in-flight must be at least 1")
	ErrNegativeDuration      = errors.New("low.idle-grace and low.max-wait must not be negative")
	ErrInvalidGenerationPath = errors.New("generation-paths entries must start with /")
	ErrPortConflict          = errors.New("metrics.port must differ from http.port")
)

// Validate checks what the struct tags can't.
func (c Config) Validate() error {
	switch c.LogLevel {
	case LogLevelDebug, LogLevelInfo, LogLevelWarn, LogLevelError:
	default:
		return ErrInvalidLogLevel
	}
	u, err := url.Parse(c.Upstream.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return ErrInvalidUpstreamURL
	}
	if c.Low.MaxInFlight < 1 {
		return ErrInvalidMaxInFlight
	}
	if c.Low.IdleGrace < 0 || c.Low.MaxWait < 0 {
		return ErrNegativeDuration
	}
	for i, p := range c.GenerationPaths {
		if !strings.HasPrefix(p, "/") {
			return fmt.Errorf("generation-paths[%d] %q: %w", i, p, ErrInvalidGenerationPath)
		}
	}
	if c.Metrics.Enabled && c.Metrics.Port == c.HTTP.Port {
		return ErrPortConflict
	}
	return nil
}
