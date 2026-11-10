package cmd

import (
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"syscall"

	"github.com/USA-RedDragon/configulator/v2"
	"github.com/USA-RedDragon/ollama-priority-proxy/internal/config"
	"github.com/USA-RedDragon/ollama-priority-proxy/internal/metrics"
	"github.com/USA-RedDragon/ollama-priority-proxy/internal/proxy"
	"github.com/USA-RedDragon/ollama-priority-proxy/internal/scheduler"
	"github.com/USA-RedDragon/ollama-priority-proxy/internal/server"
	"github.com/lmittmann/tint"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/ztrue/shutdown"
)

func NewCommand(version, commit string) *cobra.Command {
	return &cobra.Command{
		Use:     "ollama-priority-proxy",
		Short:   "Reverse proxy for Ollama that sends people's requests before background ones",
		Version: fmt.Sprintf("%s - %s", version, commit),
		Annotations: map[string]string{
			"version": version,
			"commit":  commit,
		},
		RunE:              runRoot,
		SilenceUsage:      true,
		SilenceErrors:     true,
		DisableAutoGenTag: true,
	}
}

func setupLogger(level config.LogLevel) {
	var logger *slog.Logger
	switch level {
	case config.LogLevelDebug:
		logger = slog.New(tint.NewTextHandler(os.Stdout, &tint.Options{Level: slog.LevelDebug}))
	case config.LogLevelInfo:
		logger = slog.New(tint.NewTextHandler(os.Stdout, &tint.Options{Level: slog.LevelInfo}))
	case config.LogLevelWarn:
		logger = slog.New(tint.NewTextHandler(os.Stderr, &tint.Options{Level: slog.LevelWarn}))
	case config.LogLevelError:
		logger = slog.New(tint.NewTextHandler(os.Stderr, &tint.Options{Level: slog.LevelError}))
	}
	slog.SetDefault(logger)
}

func runRoot(cmd *cobra.Command, _ []string) error {
	c, err := configulator.FromContext[config.Config](cmd.Context())
	if err != nil {
		return fmt.Errorf("failed to get config from context: %w", err)
	}
	cfg, err := c.Load()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}
	setupLogger(cfg.LogLevel)

	slog.Info("ollama-priority-proxy", "version", cmd.Annotations["version"], "commit", cmd.Annotations["commit"])
	slog.Debug("Config\n" + cfg.PrintConfig())

	upstream, err := url.Parse(cfg.Upstream.URL)
	if err != nil {
		return fmt.Errorf("failed to parse upstream URL: %w", err)
	}

	sched := scheduler.New(scheduler.Options{
		LowMaxInFlight: int(cfg.Low.MaxInFlight),
		IdleGrace:      cfg.Low.IdleGrace,
		MaxWait:        cfg.Low.MaxWait,
	})
	m, err := metrics.New(prometheus.DefaultRegisterer, sched)
	if err != nil {
		return fmt.Errorf("failed to register metrics: %w", err)
	}
	p := proxy.New(proxy.Options{
		Upstream:        upstream,
		GenerationPaths: cfg.GenerationPaths,
		Scheduler:       sched,
		Metrics:         m,
		MaxWait:         cfg.Low.MaxWait,
	})

	srv := server.New(cfg, p, prometheus.DefaultGatherer)
	if err := srv.Start(); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}
	slog.Info("Proxy started", "upstream", upstream.Redacted(), "low_max_in_flight", cfg.Low.MaxInFlight,
		"low_idle_grace", cfg.Low.IdleGrace, "low_max_wait", cfg.Low.MaxWait)

	shutdown.AddWithParam(func(sig os.Signal) {
		slog.Info("Received signal", "signal", sig)
		if err := srv.Stop(); err != nil {
			slog.Error("Failed to stop server", "error", err)
			return
		}
		slog.Info("Stopped gracefully")
	})
	shutdown.Listen(syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP)
	return nil
}
