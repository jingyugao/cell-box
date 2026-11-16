// Package traefik provides a Traefik middleware. Traefik owns all forwarding;
// this package only admits and releases requests through Cellbox's control API.
package traefik

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Config struct {
	CellboxURL     string `json:"cellboxURL,omitempty"`
	ClientID       string `json:"clientID,omitempty"`
	BoxID          string `json:"boxID,omitempty"`
	WakeTimeout    string `json:"wakeTimeout,omitempty"`
	ControlTimeout string `json:"controlTimeout,omitempty"`
}

func CreateConfig() *Config {
	return &Config{ClientID: "internal", WakeTimeout: "120s", ControlTimeout: "5s"}
}

type middleware struct {
	next           http.Handler
	client         *http.Client
	baseURL        string
	clientID       string
	boxID          string
	name           string
	wakeTimeout    time.Duration
	controlTimeout time.Duration
}

func New(ctx context.Context, next http.Handler, config *Config, name string) (http.Handler, error) {
	if config == nil {
		return nil, fmt.Errorf("cellbox: configuration is required")
	}
	base, err := url.Parse(config.CellboxURL)
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return nil, fmt.Errorf("cellbox: cellboxURL must be an HTTP(S) base URL")
	}
	if config.BoxID == "" || strings.ContainsAny(config.BoxID, "/\\\x00\r\n") || config.BoxID == "." || config.BoxID == ".." {
		return nil, fmt.Errorf("cellbox: boxID is required")
	}
	wakeTimeout, err := time.ParseDuration(config.WakeTimeout)
	if err != nil || wakeTimeout <= 0 || wakeTimeout > 10*time.Minute {
		return nil, fmt.Errorf("cellbox: wakeTimeout must be positive and at most 10m")
	}
	controlTimeout, err := time.ParseDuration(config.ControlTimeout)
	if err != nil || controlTimeout <= 0 || controlTimeout > 30*time.Second {
		return nil, fmt.Errorf("cellbox: controlTimeout must be positive and at most 30s")
	}
	return &middleware{next: next, baseURL: strings.TrimRight(config.CellboxURL, "/"), clientID: config.ClientID,
		boxID: config.BoxID, name: name, wakeTimeout: wakeTimeout, controlTimeout: controlTimeout,
		client: &http.Client{Transport: &http.Transport{Proxy: nil, MaxIdleConns: 64, MaxIdleConnsPerHost: 64, IdleConnTimeout: 30 * time.Second},
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (m *middleware) control(ctx context.Context, method, requestID string) error {
	endpoint := m.baseURL + "/v1/boxes/" + url.PathEscape(m.boxID) + "/http-requests/" + requestID
	r, err := http.NewRequestWithContext(ctx, method, endpoint, nil)
	if err != nil {
		return err
	}
	r.Header.Set("X-Cellbox-Client-ID", m.clientID)
	res, err := m.client.Do(r)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 8192))
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusNoContent {
		return fmt.Errorf("cellbox: admission control returned %d: %s", res.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func (m *middleware) release(requestID string) {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), m.controlTimeout)
		err = m.control(ctx, http.MethodDelete, requestID)
		cancel()
		if err == nil {
			return
		}
		if attempt < 2 {
			time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
		}
	}
	// The durable request deliberately remains visible and blocks suspension.
	log.Printf("cellbox middleware %s: release failed; box=%s request=%s error=%v", m.name, m.boxID, requestID, err)
}

func (m *middleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		http.Error(w, "Cellbox could not allocate a request ID", http.StatusServiceUnavailable)
		return
	}
	requestID := hex.EncodeToString(token[:])
	defer m.release(requestID)
	ctx, cancel := context.WithTimeout(r.Context(), m.wakeTimeout)
	err := m.control(ctx, http.MethodPut, requestID)
	cancel()
	if err != nil {
		log.Printf("cellbox middleware %s: admission failed; box=%s request=%s error=%v", m.name, m.boxID, requestID, err)
		http.Error(w, "Cellbox service could not become ready: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	// Forward the original request unchanged. Do not buffer bodies or retry
	// business requests, including when forwarding reports a transport error.
	// Traefik's proxy handler stays in ServeHTTP while copying SSE responses
	// and upgraded WebSocket connections. Preserve its original writer so all
	// streaming, hijacking and optimized forwarding interfaces remain intact.
	m.next.ServeHTTP(w, r)
}
