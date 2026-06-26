package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kafka-search/internal/config"
	"kafka-search/internal/handler"
	kafkaclient "kafka-search/internal/kafka"
)

func main() {
	cfg := config.Load()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	kClient := kafkaclient.NewClient(kafkaclient.ClientConfig{
		ConnTimeoutSec:  cfg.ConnTimeoutSec,
		MaxMessageBytes: cfg.MaxMessageBytes,
	})

	h := handler.New(kClient, logger)

	mux := http.NewServeMux()
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "static/index.html")
	})
	h.RegisterRoutes(mux)

	srv := &http.Server{
		Addr:         cfg.Addr,
		Handler:      mux,
		ReadTimeout:  time.Duration(cfg.ReadTimeoutSec)*time.Second + 30*time.Second,
		WriteTimeout: time.Duration(cfg.ReadTimeoutSec)*time.Second + 30*time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		logger.Info("starting", "addr", cfg.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("shutdown error", "err", err)
	}
}
