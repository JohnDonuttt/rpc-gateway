package config

import (
	"strings"
	"testing"
	"time"
)

const minimal = `{"upstreams":[{"name":"a","url":"http://127.0.0.1:8546"}]}`

func TestMinimalConfigGetsDefaults(t *testing.T) {
	cfg, err := Parse([]byte(minimal))
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Listen != ":8545" || cfg.MaxAttempts != 3 || cfg.UnhealthyAfter != 2 || cfg.MaxLagBlocks != 5 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.AttemptTimeout.Std() != 10*time.Second || cfg.HealthInterval.Std() != 5*time.Second {
		t.Fatalf("unexpected durations: %v %v", cfg.AttemptTimeout.Std(), cfg.HealthInterval.Std())
	}
}

func TestOverridesAndDurations(t *testing.T) {
	cfg, err := Parse([]byte(`{
		"listen": "127.0.0.1:9000",
		"upstreams": [{"name":"a","url":"https://x.example/key"},{"name":"b","url":"http://localhost:8545"}],
		"attemptTimeout": "250ms",
		"maxLagBlocks": 2,
		"rateLimit": {"requestsPerSecond": 50, "burst": 100},
		"trustProxyHeaders": true
	}`))
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Listen != "127.0.0.1:9000" || cfg.AttemptTimeout.Std() != 250*time.Millisecond || cfg.MaxLagBlocks != 2 {
		t.Fatalf("overrides not applied: %+v", cfg)
	}
	if cfg.RateLimit.RequestsPerSecond != 50 || !cfg.TrustProxyHeaders || len(cfg.Upstreams) != 2 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestRejectsInvalidConfigs(t *testing.T) {
	tests := []struct {
		name string
		json string
		want string
	}{
		{"no upstreams", `{"upstreams":[]}`, "at least one upstream"},
		{"missing name", `{"upstreams":[{"url":"http://a"}]}`, "no name"},
		{"duplicate names", `{"upstreams":[{"name":"a","url":"http://a"},{"name":"a","url":"http://b"}]}`, "duplicate"},
		{"bad scheme", `{"upstreams":[{"name":"a","url":"ftp://a"}]}`, "invalid url"},
		{"no host", `{"upstreams":[{"name":"a","url":"http://"}]}`, "invalid url"},
		{"bad duration", `{"upstreams":[{"name":"a","url":"http://a"}],"attemptTimeout":"fast"}`, "invalid duration"},
		{"numeric duration", `{"upstreams":[{"name":"a","url":"http://a"}],"attemptTimeout":5}`, "must be a string"},
		{"zero attempts", `{"upstreams":[{"name":"a","url":"http://a"}],"maxAttempts":0}`, "at least 1"},
		{"limit without burst", `{"upstreams":[{"name":"a","url":"http://a"}],"rateLimit":{"requestsPerSecond":5}}`, "burst"},
		{"typo in a key", `{"upstream":[{"name":"a","url":"http://a"}]}`, "unknown field"},
		{"not json", `nope`, "parsing config"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.json))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tt.want)
			}
		})
	}
}
