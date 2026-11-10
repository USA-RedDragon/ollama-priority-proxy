package config_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/USA-RedDragon/configulator/v2"
	"github.com/USA-RedDragon/ollama-priority-proxy/internal/config"
	"github.com/goccy/go-yaml"
)

const apiChat = "/api/chat"

func load(t *testing.T, env map[string]string, file string) (*config.Config, error) {
	t.Helper()
	return configulator.New(config.ConfigSchema()).
		WithEnvironmentVariables(&configulator.EnvironmentVariableOptions{Separator: "_"}).
		WithFile(&configulator.FileOptions{
			Search:   []string{"config.yaml"},
			Decoders: configulator.Decoders{".yaml": yaml.Unmarshal},
		}).
		WithEnviron(func(k string) (string, bool) {
			v, ok := env[k]
			return v, ok
		}).
		WithReadFile(func(string) ([]byte, error) { return []byte(file), nil }).
		Load()
}

func defaults(t *testing.T) config.Config {
	t.Helper()
	cfg, err := configulator.New(config.ConfigSchema()).Default()
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestDefaults(t *testing.T) {
	t.Parallel()
	cfg := defaults(t)
	want := config.Config{
		LogLevel: config.LogLevelInfo,
		HTTP:     config.HTTP{Bind: "[::]", Port: 11434},
		Metrics:  config.Metrics{Bind: "[::]", Port: 9090},
		Upstream: config.Upstream{URL: "http://127.0.0.1:11434"},
		Low:      config.Low{MaxInFlight: 1, IdleGrace: 2 * time.Second},
		GenerationPaths: []string{
			"/v1/chat/completions", "/v1/completions", "/v1/embeddings", "/v1/responses",
			apiChat, "/api/generate", "/api/embed", "/api/embeddings",
		},
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("defaults:\n got %+v\nwant %+v", cfg, want)
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("defaults do not validate: %v", err)
	}
}

func TestLoadFile(t *testing.T) {
	t.Parallel()
	cfg, err := load(t, nil, `
log-level: debug
upstream:
  url: http://gpu.example.com:11434
low:
  max-in-flight: 2
  idle-grace: 500ms
  max-wait: 10m
generation-paths:
  - /api/chat
`)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LogLevel != config.LogLevelDebug || cfg.Upstream.URL != "http://gpu.example.com:11434" ||
		cfg.Low != (config.Low{MaxInFlight: 2, IdleGrace: 500 * time.Millisecond, MaxWait: 10 * time.Minute}) ||
		!reflect.DeepEqual(cfg.GenerationPaths, []string{apiChat}) {
		t.Errorf("file: %+v", cfg)
	}
}

func TestEnvNames(t *testing.T) {
	t.Parallel()
	cfg, err := load(t, map[string]string{
		"LOG_LEVEL":         "warn",
		"HTTP_PORT":         "8080",
		"METRICS_ENABLED":   "false",
		"UPSTREAM_URL":      "https://ollama.example.com",
		"LOW_MAX_IN_FLIGHT": "3",
		"LOW_IDLE_GRACE":    "0s",
		"LOW_MAX_WAIT":      "1m",
		"GENERATION_PATHS":  "/api/chat,/api/generate",
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LogLevel != config.LogLevelWarn || cfg.HTTP.Port != 8080 || cfg.Metrics.Enabled ||
		cfg.Upstream.URL != "https://ollama.example.com" ||
		cfg.Low != (config.Low{MaxInFlight: 3, MaxWait: time.Minute}) ||
		!reflect.DeepEqual(cfg.GenerationPaths, []string{apiChat, "/api/generate"}) {
		t.Errorf("env: %+v", cfg)
	}
}

func TestLoadExampleFile(t *testing.T) {
	t.Parallel()
	cfg, err := configulator.New(config.ConfigSchema()).
		WithFile(&configulator.FileOptions{
			Explicit: "../../config.example.yaml",
			Decoders: configulator.Decoders{".yaml": yaml.Unmarshal},
		}).
		Load()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*cfg, defaults(t)) {
		t.Errorf("config.example.yaml should load the defaults:\n got %+v\nwant %+v", *cfg, defaults(t))
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		change func(*config.Config)
		want   error
	}{
		{"log level", func(c *config.Config) { c.LogLevel = "trace" }, config.ErrInvalidLogLevel},
		{"no scheme", func(c *config.Config) { c.Upstream.URL = "ollama:11434" }, config.ErrInvalidUpstreamURL},
		{"no host", func(c *config.Config) { c.Upstream.URL = "http://" }, config.ErrInvalidUpstreamURL},
		{"max in flight", func(c *config.Config) { c.Low.MaxInFlight = 0 }, config.ErrInvalidMaxInFlight},
		{"grace", func(c *config.Config) { c.Low.IdleGrace = -time.Second }, config.ErrNegativeDuration},
		{"max wait", func(c *config.Config) { c.Low.MaxWait = -time.Second }, config.ErrNegativeDuration},
		{"path", func(c *config.Config) { c.GenerationPaths = []string{"api/chat"} }, config.ErrInvalidGenerationPath},
		{"ports", func(c *config.Config) { c.Metrics.Enabled = true; c.Metrics.Port = c.HTTP.Port }, config.ErrPortConflict},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := defaults(t)
			cfg.GenerationPaths = append([]string(nil), cfg.GenerationPaths...)
			tc.change(&cfg)
			if err := cfg.Validate(); !errors.Is(err, tc.want) {
				t.Errorf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestLoadRejectsInvalid(t *testing.T) {
	t.Parallel()
	if _, err := load(t, map[string]string{"LOW_MAX_IN_FLIGHT": "0"}, ""); !errors.Is(err, config.ErrInvalidMaxInFlight) {
		t.Errorf("err = %v", err)
	}
}
