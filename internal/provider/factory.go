// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"fmt"
	"net/http"
	"time"

	"github.com/ori-platform/ori-gateway/internal/config"
)

// NewFromConfig constructs the selected Tier 3 reasoning provider.
//
// This factory is intentionally scoped to runtime reasoning requests. Customer
// reporting and analytics providers are built through separate product-layer
// factories and must not be wired here.
func NewFromConfig(cfg config.ProviderConfig) (Provider, error) {
	if cfg.TimeoutMS < 0 {
		return nil, fmt.Errorf("provider.timeout_ms must not be negative")
	}
	switch cfg.Name {
	case config.ProviderEcho:
		return EchoProvider{}, nil
	case config.ProviderLlamaCpp:
		return NewLlamaCppProvider(LlamaCppOptions{
			URL:           cfg.LlamaCpp.URL,
			ModelFallback: cfg.LlamaCpp.Model,
			HTTPClient: &http.Client{
				Timeout: providerTimeout(cfg.TimeoutMS),
			},
		})
	case config.ProviderCloudLLM:
		return nil, fmt.Errorf("provider %q is known but not wired for Tier 3 reasoning yet", cfg.Name)
	case "":
		return nil, fmt.Errorf("provider name must not be empty")
	default:
		return nil, fmt.Errorf("provider %q is unknown", cfg.Name)
	}
}

// providerTimeout treats zero as omitted config and applies the loader default.
func providerTimeout(timeoutMS int) time.Duration {
	if timeoutMS == 0 {
		return time.Duration(config.DefaultProviderTimeoutMS) * time.Millisecond
	}
	return time.Duration(timeoutMS) * time.Millisecond
}
