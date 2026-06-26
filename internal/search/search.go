package search

import (
	"encoding/json"
	"fmt"
	"strings"
)

type Request struct {
	Brokers         []string          `json:"brokers"`
	BrokerOverrides map[string]string `json:"broker_overrides"`
	Topic           string            `json:"topic"`
	Query           string            `json:"query"`
	Field           string            `json:"field"`
	FromTimestamp   int64             `json:"from_timestamp"` // unix ms, 0 = beginning
	Limit           int               `json:"limit"`
}

type Message struct {
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Timestamp string `json:"timestamp"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

type Result struct {
	Messages           []Message `json:"messages"`
	Scanned            int       `json:"scanned"`
	PartitionsSearched int       `json:"partitions_searched"`
	PartitionsTotal    int       `json:"partitions_total"`
	Warning            string    `json:"warning,omitempty"`
	Error              string    `json:"error,omitempty"`
}

// MatchesFilter returns true if value satisfies the query.
// If field is empty — substring match on raw bytes.
// If field is set — exact match on the JSON field value (dot-separated path supported).
func MatchesFilter(value []byte, query, field string) bool {
	if query == "" {
		return true
	}
	if field == "" {
		return strings.Contains(string(value), query)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(value, &m); err != nil {
		return false
	}
	val, ok := nestedField(m, field)
	if !ok {
		return false
	}
	return fmt.Sprintf("%v", val) == query
}

func nestedField(m map[string]interface{}, path string) (interface{}, bool) {
	parts := strings.SplitN(path, ".", 2)
	val, ok := m[parts[0]]
	if !ok {
		return nil, false
	}
	if len(parts) == 1 {
		return val, true
	}
	nested, ok := val.(map[string]interface{})
	if !ok {
		return nil, false
	}
	return nestedField(nested, parts[1])
}
