package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"time"

	"kafka-search/internal/kafka"
	"kafka-search/internal/search"
)

// KafkaClient is the interface handler depends on — makes it easy to test.
type KafkaClient interface {
	ListTopics(ctx context.Context, broker string) ([]string, error)
	ResolvePartitions(ctx context.Context, broker, topic string, overrides map[string]string) (accessible []kafka.PartitionMeta, skipped []int, total int, err error)
	ReadPartition(ctx context.Context, req search.Request, partition int, leaderAddr string) kafka.PartitionResult
}

type Handler struct {
	kafka  KafkaClient
	logger *slog.Logger
}

func New(k KafkaClient, logger *slog.Logger) *Handler {
	return &Handler{kafka: k, logger: logger}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/topics", h.handleTopics)
	mux.HandleFunc("/api/search", h.handleSearch)
}

func (h *Handler) handleTopics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	broker := r.URL.Query().Get("brokers")
	if broker == "" {
		writeError(w, http.StatusBadRequest, "brokers query param required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	topics, err := h.kafka.ListTopics(ctx, broker)
	if err != nil {
		h.logger.Error("list topics", "broker", broker, "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"topics": topics})
}

func (h *Handler) handleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req search.Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if err := validateSearchRequest(req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Limit <= 0 {
		req.Limit = 100
	}

	ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
	defer cancel()

	start := time.Now()
	result := h.runSearch(ctx, req)
	h.logger.Info("search done",
		"topic", req.Topic,
		"scanned", result.Scanned,
		"found", len(result.Messages),
		"duration", time.Since(start).Round(time.Millisecond),
	)

	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) runSearch(ctx context.Context, req search.Request) search.Result {
	accessible, skipped, total, err := h.kafka.ResolvePartitions(ctx, req.Brokers[0], req.Topic, req.BrokerOverrides)
	if err != nil {
		return search.Result{Error: err.Error()}
	}

	type chanResult struct {
		msgs    []search.Message
		scanned int
		err     error
	}

	ch := make(chan chanResult, len(accessible))

	for _, p := range accessible {
		go func(pm kafka.PartitionMeta) {
			pr := h.kafka.ReadPartition(ctx, req, pm.ID, pm.LeaderAddr)
			ch <- chanResult{msgs: pr.Msgs, scanned: pr.Scanned, err: pr.Err}
		}(p)
	}

	var all []search.Message
	totalScanned := 0
	var firstErr string

	for range accessible {
		pr := <-ch
		if pr.err != nil {
			h.logger.Warn("partition read error", "err", pr.err)
			if firstErr == "" {
				firstErr = pr.err.Error()
			}
		}
		all = append(all, pr.msgs...)
		totalScanned += pr.scanned
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Timestamp < all[j].Timestamp
	})
	if len(all) > req.Limit {
		all = all[:req.Limit]
	}
	if all == nil {
		all = []search.Message{}
	}

	var warning string
	if len(skipped) > 0 {
		warning = fmt.Sprintf("Partitions %v skipped — their leaders are not accessible. Results may be incomplete.", skipped)
	}

	return search.Result{
		Messages:           all,
		Scanned:            totalScanned,
		PartitionsSearched: len(accessible),
		PartitionsTotal:    total,
		Warning:            warning,
		Error:              firstErr,
	}
}

func validateSearchRequest(req search.Request) error {
	if len(req.Brokers) == 0 || req.Brokers[0] == "" {
		return fmt.Errorf("brokers required")
	}
	if req.Topic == "" {
		return fmt.Errorf("topic required")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
