package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/oxia-io/okk/coordinator/internal/api"
	"github.com/oxia-io/okk/coordinator/internal/task"
	"github.com/spf13/cobra"
)

var (
	listenAddr string
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "okk-coordinator",
		Short: "OKK test coordinator — generates operations and streams them to workers via gRPC",
		RunE:  run,
	}

	rootCmd.Flags().StringVar(&listenAddr, "listen", ":8080", "HTTP listen address")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(_ *cobra.Command, _ []string) error {
	slog.Info("Starting okk-coordinator", "listen", listenAddr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager := task.NewManager(ctx)
	server := api.NewServer(manager)

	httpServer := &http.Server{
		Addr:    listenAddr,
		Handler: server.Handler(),
	}

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		sig := <-sigCh
		slog.Info("Received signal, shutting down", "signal", sig)
		cancel()

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			slog.Error("HTTP server shutdown error", "error", err)
		}
	}()

	slog.Info(fmt.Sprintf("HTTP server listening on %s", listenAddr))
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("HTTP server error: %w", err)
	}

	return nil
}
