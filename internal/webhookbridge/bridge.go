// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package webhookbridge

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/ori-platform/ori-gateway/internal/config"
)

const signaturePrefix = "hmac-sha256:"

type Options struct {
	Now        func() time.Time
	Nonce      func() (string, error)
	Getenv     func(string) string
	HTTPClient *http.Client
	Logger     *slog.Logger
}

type Server struct {
	listenAddr       string
	path             string
	targetURL        string
	providerPrefixes []netip.Prefix
	loopbackOnly     bool
	runtimeToken     string
	hmacSecret       string
	maxBodyBytes     int64
	httpClient       *http.Client
	logger           *slog.Logger
	now              func() time.Time
	nonce            func() (string, error)
}

func New(cfg config.WebhookBridgeConfig, opts Options) (*Server, error) {
	if !cfg.Enabled {
		return nil, fmt.Errorf("webhook bridge is disabled")
	}
	getenv := opts.Getenv
	if getenv == nil {
		getenv = os.Getenv
	}
	runtimeToken := strings.TrimSpace(getenv(cfg.RuntimeTokenEnv))
	if runtimeToken == "" {
		return nil, fmt.Errorf("webhook_bridge.runtime_token_env %q is empty", cfg.RuntimeTokenEnv)
	}
	hmacSecret := strings.TrimSpace(getenv(cfg.HMACSecretEnv))
	if hmacSecret == "" {
		return nil, fmt.Errorf("webhook_bridge.hmac_secret_env %q is empty", cfg.HMACSecretEnv)
	}
	if _, err := url.ParseRequestURI(cfg.TargetURL); err != nil {
		return nil, fmt.Errorf("webhook_bridge.target_url is invalid: %w", err)
	}
	prefixes := make([]netip.Prefix, 0, len(cfg.ProviderSourceCIDRs))
	for _, cidr := range cfg.ProviderSourceCIDRs {
		prefix, err := netip.ParsePrefix(cidr)
		if err != nil {
			return nil, fmt.Errorf("parse provider CIDR %q: %w", cidr, err)
		}
		prefixes = append(prefixes, prefix)
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	nonce := opts.Nonce
	if nonce == nil {
		nonce = randomNonce
	}
	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: time.Duration(cfg.RequestTimeoutMS) * time.Millisecond}
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		listenAddr:       cfg.ListenAddr,
		path:             cfg.Path,
		targetURL:        cfg.TargetURL,
		providerPrefixes: prefixes,
		loopbackOnly:     len(prefixes) == 0,
		runtimeToken:     runtimeToken,
		hmacSecret:       hmacSecret,
		maxBodyBytes:     cfg.MaxBodyBytes,
		httpClient:       client,
		logger:           logger,
		now:              now,
		nonce:            nonce,
	}, nil
}

func (s *Server) Run(ctx context.Context) error {
	server := &http.Server{
		Addr:              s.listenAddr,
		Handler:           s,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		err := server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			_ = server.Close()
			<-errCh
			return fmt.Errorf("shutdown webhook bridge: %w", err)
		}
		<-errCh
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != s.path {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.sourceAllowed(r.RemoteAddr) {
		s.logger.Warn("webhook bridge rejected source", "remote_addr", redactHost(r.RemoteAddr))
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	defer r.Body.Close()
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.maxBodyBytes))
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	signedAtMS := s.now().UnixMilli()
	nonce, err := s.nonce()
	if err != nil {
		s.logger.Error("webhook bridge nonce generation failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	forwardReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost, s.targetURL, bytes.NewReader(body))
	if err != nil {
		s.logger.Error("webhook bridge request construction failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if contentType := r.Header.Get("Content-Type"); contentType != "" {
		forwardReq.Header.Set("Content-Type", contentType)
	}
	forwardReq.Header.Set("X-Ori-Webhook-Token", s.runtimeToken)
	forwardReq.Header.Set("X-Ori-Webhook-Signature", signBody(body, s.hmacSecret, signedAtMS, nonce))
	forwardReq.Header.Set("X-Ori-Webhook-Timestamp", fmt.Sprintf("%d", signedAtMS))
	forwardReq.Header.Set("X-Ori-Webhook-Nonce", nonce)

	resp, err := s.httpClient.Do(forwardReq)
	if err != nil {
		s.logger.Warn("webhook bridge runtime forward failed", "error", err)
		http.Error(w, "runtime webhook unavailable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		s.logger.Warn("webhook bridge runtime rejected request", "status", resp.StatusCode)
		http.Error(w, "runtime webhook rejected request", http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *Server) sourceAllowed(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return false
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	if s.loopbackOnly {
		return addr.IsLoopback()
	}
	for _, prefix := range s.providerPrefixes {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

func signBody(body []byte, secret string, signedAtMS int64, nonce string) string {
	signed := bytes.Join([][]byte{
		[]byte(fmt.Sprintf("%d", signedAtMS)),
		[]byte(nonce),
		body,
	}, []byte("\n"))
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(signed)
	return signaturePrefix + hex.EncodeToString(mac.Sum(nil))
}

func randomNonce() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func redactHost(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return "invalid"
	}
	return host
}
