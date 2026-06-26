package config

import (
	"flag"
	"os"
	"strconv"
)

type Config struct {
	Addr              string
	ReadTimeoutSec    int
	ConnTimeoutSec    int
	MaxMessageBytes   int
}

func Load() Config {
	c := Config{
		Addr:            ":8080",
		ReadTimeoutSec:  120,
		ConnTimeoutSec:  15,
		MaxMessageBytes: 10 * 1024 * 1024,
	}

	flag.StringVar(&c.Addr, "addr", envOr("ADDR", c.Addr), "listen address")
	flag.IntVar(&c.ReadTimeoutSec, "read-timeout", envIntOr("READ_TIMEOUT_SEC", c.ReadTimeoutSec), "kafka read timeout seconds")
	flag.IntVar(&c.ConnTimeoutSec, "conn-timeout", envIntOr("CONN_TIMEOUT_SEC", c.ConnTimeoutSec), "kafka connect timeout seconds")
	flag.Parse()

	return c
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envIntOr(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
