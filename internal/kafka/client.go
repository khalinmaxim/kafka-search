package kafka

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"kafka-search/internal/search"
)

type ClientConfig struct {
	ConnTimeoutSec  int
	MaxMessageBytes int
}

type Client struct {
	cfg ClientConfig
}

func NewClient(cfg ClientConfig) *Client {
	return &Client{cfg: cfg}
}

// ListTopics returns sorted topic names visible from the given broker.
func (c *Client) ListTopics(ctx context.Context, broker string) ([]string, error) {
	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", broker, err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(time.Duration(c.cfg.ConnTimeoutSec) * time.Second))

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, fmt.Errorf("read partitions: %w", err)
	}

	seen := make(map[string]struct{}, len(partitions))
	for _, p := range partitions {
		seen[p.Topic] = struct{}{}
	}
	topics := make([]string, 0, len(seen))
	for t := range seen {
		topics = append(topics, t)
	}
	sort.Strings(topics)
	return topics, nil
}

type PartitionMeta struct {
	ID         int
	LeaderAddr string // resolved via overrides
}

// ResolvePartitions fetches partition metadata and splits them into
// accessible (leader reachable) and skipped (leader not reachable) lists.
func (c *Client) ResolvePartitions(
	ctx context.Context,
	broker, topic string,
	overrides map[string]string,
) (accessible []PartitionMeta, skipped []int, total int, err error) {
	dialer := newDialer(overrides, c.cfg.ConnTimeoutSec)

	connCtx, cancel := context.WithTimeout(ctx, time.Duration(c.cfg.ConnTimeoutSec)*time.Second)
	conn, err := dialer.DialContext(connCtx, "tcp", broker)
	cancel()
	if err != nil {
		return nil, nil, 0, fmt.Errorf("dial %s: %w", broker, err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("read partitions for %s: %w", topic, err)
	}
	total = len(partitions)

	accessibleAddrs := buildAccessibleSet(broker, overrides)

	for _, p := range partitions {
		leaderAddr := resolveAddr(p.Leader.Host, p.Leader.Port, overrides)
		if accessibleAddrs[leaderAddr] {
			accessible = append(accessible, PartitionMeta{ID: p.ID, LeaderAddr: leaderAddr})
		} else {
			skipped = append(skipped, p.ID)
		}
	}
	return accessible, skipped, total, nil
}

type PartitionResult struct {
	Msgs    []search.Message
	Scanned int
	Err     error
}

// ReadPartition reads messages from a single partition and returns those matching the filter.
func (c *Client) ReadPartition(ctx context.Context, req search.Request, partition int, leaderAddr string) PartitionResult {
	dialer := newDialer(req.BrokerOverrides, c.cfg.ConnTimeoutSec)

	connCtx, cancel := context.WithTimeout(ctx, time.Duration(c.cfg.ConnTimeoutSec)*time.Second)
	conn, err := dialer.DialLeader(connCtx, "tcp", leaderAddr, req.Topic, partition)
	cancel()
	if err != nil {
		return PartitionResult{Err: fmt.Errorf("partition %d: dial leader: %w", partition, err)}
	}

	endOffset, err := conn.ReadLastOffset()
	if err != nil {
		conn.Close()
		return PartitionResult{Err: fmt.Errorf("partition %d: last offset: %w", partition, err)}
	}

	startOffset, err := c.resolveStartOffset(conn, req.FromTimestamp)
	conn.Close()
	if err != nil {
		return PartitionResult{Err: fmt.Errorf("partition %d: start offset: %w", partition, err)}
	}

	if startOffset >= endOffset {
		return PartitionResult{}
	}

	return c.readMessages(ctx, req, partition, leaderAddr, startOffset, endOffset, dialer)
}

func (c *Client) resolveStartOffset(conn *kafka.Conn, fromTimestampMs int64) (int64, error) {
	if fromTimestampMs > 0 {
		off, err := conn.ReadOffset(time.UnixMilli(fromTimestampMs))
		if err == nil {
			return off, nil
		}
	}
	return conn.ReadFirstOffset()
}

func (c *Client) readMessages(
	ctx context.Context,
	req search.Request,
	partition int,
	leaderAddr string,
	startOffset, endOffset int64,
	dialer *kafka.Dialer,
) PartitionResult {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{leaderAddr},
		Topic:     req.Topic,
		Partition: partition,
		Dialer:    dialer,
		MinBytes:  1,
		MaxBytes:  c.cfg.MaxMessageBytes,
		MaxWait:   1 * time.Second,
	})
	defer r.Close()

	if err := r.SetOffset(startOffset); err != nil {
		return PartitionResult{Err: fmt.Errorf("partition %d: set offset: %w", partition, err)}
	}

	var msgs []search.Message
	scanned := 0

	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			break
		}
		scanned++

		if search.MatchesFilter(msg.Value, req.Query, req.Field) {
			msgs = append(msgs, search.Message{
				Partition: partition,
				Offset:    msg.Offset,
				Timestamp: msg.Time.Format("2006-01-02 15:04:05"),
				Key:       string(msg.Key),
				Value:     string(msg.Value),
			})
		}

		if msg.Offset >= endOffset-1 || len(msgs) >= req.Limit {
			break
		}
	}

	return PartitionResult{Msgs: msgs, Scanned: scanned}
}

// --- dialer helpers ---

type mappingDialer struct {
	mapping map[string]string
}

func (d *mappingDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if mapped, ok := d.mapping[addr]; ok {
		addr = mapped
	}
	return (&net.Dialer{}).DialContext(ctx, network, addr)
}

func newDialer(overrides map[string]string, timeoutSec int) *kafka.Dialer {
	if len(overrides) == 0 {
		return kafka.DefaultDialer
	}
	return &kafka.Dialer{
		Timeout:   time.Duration(timeoutSec) * time.Second,
		DualStack: true,
		DialFunc:  (&mappingDialer{mapping: overrides}).DialContext,
	}
}

func resolveAddr(host string, port int, overrides map[string]string) string {
	addr := fmt.Sprintf("%s:%d", host, port)
	if mapped, ok := overrides[addr]; ok {
		return mapped
	}
	return addr
}

func buildAccessibleSet(seedBroker string, overrides map[string]string) map[string]bool {
	set := make(map[string]bool)
	if !strings.Contains(seedBroker, ":") {
		seedBroker += ":9092"
	}
	set[seedBroker] = true
	for _, dst := range overrides {
		set[dst] = true
	}
	return set
}
