// Package config loads and validates the gateway's JSON config file.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"
)

// Duration lets the config use strings like "5s" or "250ms".
type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string like \"5s\": %w", err)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(parsed)
	return nil
}

func (d Duration) Std() time.Duration { return time.Duration(d) }

type Upstream struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

type RateLimit struct {
	RequestsPerSecond float64 `json:"requestsPerSecond"`
	Burst             int     `json:"burst"`
}

type Config struct {
	Listen    string     `json:"listen"`
	Upstreams []Upstream `json:"upstreams"`

	AttemptTimeout Duration `json:"attemptTimeout"`
	MaxAttempts    int      `json:"maxAttempts"`

	HealthInterval Duration `json:"healthInterval"`
	UnhealthyAfter int      `json:"unhealthyAfter"`
	MaxLagBlocks   uint64   `json:"maxLagBlocks"`

	CacheEntries      int       `json:"cacheEntries"`
	RateLimit         RateLimit `json:"rateLimit"`
	TrustProxyHeaders bool      `json:"trustProxyHeaders"`
}

func defaults() Config {
	return Config{
		Listen:         ":8545",
		AttemptTimeout: Duration(10 * time.Second),
		MaxAttempts:    3,
		HealthInterval: Duration(5 * time.Second),
		UnhealthyAfter: 2,
		MaxLagBlocks:   5,
		CacheEntries:   10000,
	}
}

func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	return Parse(data)
}

func Parse(data []byte) (Config, error) {
	cfg := defaults()

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields() // a typo like "upstream" shouldn't silently do nothing
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("parsing config: %w", err)
	}
	return cfg, cfg.validate()
}

func (c Config) validate() error {
	if len(c.Upstreams) == 0 {
		return errors.New("config: at least one upstream is required")
	}

	seen := map[string]bool{}
	for i, u := range c.Upstreams {
		if u.Name == "" {
			return fmt.Errorf("config: upstreams[%d] has no name", i)
		}
		if seen[u.Name] {
			return fmt.Errorf("config: duplicate upstream name %q", u.Name)
		}
		seen[u.Name] = true

		parsed, err := url.Parse(u.URL)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			return fmt.Errorf("config: upstream %q has an invalid url (need http:// or https://)", u.Name)
		}
	}

	if c.AttemptTimeout.Std() <= 0 || c.HealthInterval.Std() <= 0 {
		return errors.New("config: attemptTimeout and healthInterval must be positive")
	}
	if c.MaxAttempts < 1 || c.UnhealthyAfter < 1 {
		return errors.New("config: maxAttempts and unhealthyAfter must be at least 1")
	}
	if c.RateLimit.RequestsPerSecond < 0 || c.RateLimit.Burst < 0 {
		return errors.New("config: rateLimit values can't be negative")
	}
	if c.RateLimit.RequestsPerSecond > 0 && c.RateLimit.Burst < 1 {
		return errors.New("config: rateLimit.burst must be at least 1 when requestsPerSecond is set")
	}
	return nil
}
