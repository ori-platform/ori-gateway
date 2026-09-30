// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/evidence/courier"
)

const (
	DefaultHeartbeatIntervalS   = 30
	DefaultProviderTimeoutMS    = 10000
	DefaultGatewayAuthSecretEnv = "GATEWAY_SHARED_SECRET"
	DefaultEvidenceMaxItems     = 10000
	DefaultEvidenceMaxBytes     = int64(256 << 20)
	DefaultEvidenceBackoffMaxS  = 300

	DefaultEvidenceStoreProbeIntervalS = 900
	MinEvidenceStoreProbeIntervalS     = 300
	MaxEvidenceStoreProbeIntervalS     = 900
	DefaultEvidenceRetryS              = 5

	DefaultWebhookBridgeListenAddr       = "127.0.0.1:8090"
	DefaultSiteHealthListenAddr          = "127.0.0.1:8765"
	DefaultWebhookBridgePath             = "/webhooks/sms/africastalking"
	DefaultWebhookBridgeRequestTimeoutMS = 3000
	DefaultWebhookBridgeMaxBodyBytes     = 65536

	ProviderEcho     = "echo"
	ProviderLlamaCpp = "llama_cpp"
	ProviderCloudLLM = "cloud_llm"

	CloudVendorClaude           = "claude"
	CloudVendorOpenAI           = "openai"
	CloudVendorGemini           = "gemini"
	CloudVendorDeepSeek         = "deepseek"
	CloudVendorOpenAICompatible = "openai_compatible"

	ReportingProviderGemini = "gemini"
)

// Config is the root gateway configuration loaded from gateway.yaml.
type Config struct {
	Gateway       GatewayConfig       `yaml:"gateway"`
	Provider      ProviderConfig      `yaml:"provider"`
	Reporting     ReportingConfig     `yaml:"reporting"`
	WebhookBridge WebhookBridgeConfig `yaml:"webhook_bridge"`
	SIM           SIMConfig           `yaml:"sim"`
	Fleet         FleetConfig         `yaml:"fleet"`
	SiteHealth    SiteHealthConfig    `yaml:"site_health"`
	Evidence      EvidenceConfig      `yaml:"evidence"`
}

type GatewayConfig struct {
	BrokerURL          string                  `yaml:"broker_url"`
	DeviceIDs          []string                `yaml:"device_ids"`
	HeartbeatIntervalS int                     `yaml:"heartbeat_interval_s"`
	Auth               GatewayAuthConfig       `yaml:"auth"`
	Custody            GatewayCustodyConfig    `yaml:"custody"`
	Encryption         GatewayEncryptionConfig `yaml:"encryption"`
}

type GatewayAuthConfig struct {
	Enabled                 bool   `yaml:"enabled"`
	SharedSecretEnv         string `yaml:"shared_secret_env"`
	PreviousSharedSecretEnv string `yaml:"previous_shared_secret_env"`
}

// GatewayCustodyConfig names the secret this gateway authenticates custody
// acknowledgements with.
//
// A section of its own rather than a field under auth, deliberately. Custody
// uses key material distinct from the runtime-gateway envelope secret: both are
// symmetric secrets between the same two parties, which is exactly why they
// must not be the same bytes, since domain separation makes the preimages
// differ but does not stop a component holding the secret for one purpose from
// minting artifacts for the other. Folding this into auth would invite the
// reuse the separation exists to prevent.
//
// There is no key_id here. The identifier is derived from the secret, and
// accepting a configured one would let a value name a generation it was not
// derived from.
type GatewayCustodyConfig struct {
	SecretEnv string `yaml:"secret_env"`
}

type GatewayEncryptionConfig struct {
	Enabled bool `yaml:"enabled"`
}

type EvidenceConfig struct {
	Enabled              bool   `yaml:"enabled"`
	QueueDirectory       string `yaml:"queue_directory"`
	ReturnQueueDirectory string `yaml:"return_queue_directory"`
	MaxItems             int    `yaml:"max_items"`
	MaxBytes             int64  `yaml:"max_bytes"`
	RetryIntervalS       int    `yaml:"retry_interval_s"`
	// BackoffBaseS and BackoffMaxS are the courier's back-off base and bound.
	// evidence-transport/v2 requires bound >= base >= delivery interval, which
	// the loader enforces when a device declares gateway-evidence-carriage/v1.
	BackoffBaseS int `yaml:"backoff_base_s"`
	BackoffMaxS  int `yaml:"backoff_max_s"`
	// StoreProbeIntervalS is how often each durable evidence store is probed,
	// 300 through 900 seconds when a device declares gateway-evidence-carriage/v1
	// (gateway-config/v2).
	StoreProbeIntervalS int    `yaml:"store_probe_interval_s"`
	EndpointEnv         string `yaml:"endpoint_env"`
	ClientIDEnv         string `yaml:"client_id_env"`
	SecretEnv           string `yaml:"secret_env"`
	// AuthorityCAFile is an absolute path to a PEM bundle of CA certificates;
	// when set, the courier trusts only these for the authority's endpoint.
	AuthorityCAFile string `yaml:"authority_ca_file"`
	// DeviceCarriage is each configured device's declared inbound evidence
	// carriage, every configured device named, after defaults
	// (gateway-config/v2). Nil while the courier is disabled.
	DeviceCarriage map[string]string
}

// The two evidence-carriage contracts a device may declare in
// evidence.device_carriage.
const (
	CarriageGatewayAPIV1       = "gateway-api/v1"
	CarriageEvidenceCarriageV1 = "gateway-evidence-carriage/v1"
)

// DeclaresVersionedCarriage reports whether any configured device declares
// gateway-evidence-carriage/v1, the condition that activates the versioned
// carriage's rules.
func (c EvidenceConfig) DeclaresVersionedCarriage() bool {
	for _, carriage := range c.DeviceCarriage {
		if carriage == CarriageEvidenceCarriageV1 {
			return true
		}
	}
	return false
}

// VersionedDevices lists the configured devices that declare
// gateway-evidence-carriage/v1.
func (c EvidenceConfig) VersionedDevices() []string {
	var out []string
	for device, carriage := range c.DeviceCarriage {
		if carriage == CarriageEvidenceCarriageV1 {
			out = append(out, device)
		}
	}
	slices.Sort(out)
	return out
}

// StoreProbeInterval is the interval each durable evidence store is probed
// at. store_probe_interval_s is consumed only once a device declares
// gateway-evidence-carriage/v1, when the loader has held it to its range; a
// site that declares nothing probes at the default, whatever it carries
// (gateway-config/v2: the key is a design target for such a site).
func (c EvidenceConfig) StoreProbeInterval() time.Duration {
	if !c.DeclaresVersionedCarriage() {
		return DefaultEvidenceStoreProbeIntervalS * time.Second
	}
	return time.Duration(c.StoreProbeIntervalS) * time.Second
}

// Backoff is the courier's back-off base and bound. backoff_base_s and
// backoff_max_s are consumed only once a device declares
// gateway-evidence-carriage/v1, when the loader has held them to their
// ordering; a site that declares nothing backs off from its delivery interval
// to the default bound, whatever it carries.
func (c EvidenceConfig) Backoff() (base, bound time.Duration) {
	if !c.DeclaresVersionedCarriage() {
		base = time.Duration(c.RetryIntervalS) * time.Second
		return base, max(DefaultEvidenceBackoffMaxS*time.Second, base)
	}
	return time.Duration(c.BackoffBaseS) * time.Second, time.Duration(c.BackoffMaxS) * time.Second
}

type ProviderConfig struct {
	Name      string         `yaml:"name"`
	TimeoutMS int            `yaml:"timeout_ms"`
	LlamaCpp  LlamaCppConfig `yaml:"llama_cpp"`
	CloudLLM  CloudLLMConfig `yaml:"cloud_llm"`
}

type LlamaCppConfig struct {
	URL string `yaml:"url"`
	// Model is the fallback model name used when llama.cpp /props is unreachable or returns no model name.
	Model string `yaml:"model"`
}

type CloudLLMConfig struct {
	Vendor    string `yaml:"vendor"`
	APIKeyEnv string `yaml:"api_key_env"`
	Model     string `yaml:"model"`
	BaseURL   string `yaml:"base_url"`
}

// ReportingConfig configures customer-facing report and enrichment providers.
// It is intentionally separate from ProviderConfig, which handles Tier 3 reasoning.
type ReportingConfig struct {
	Provider        string                `yaml:"provider"`
	Gemini          ReportingGeminiConfig `yaml:"gemini"`
	WeeklyReport    WeeklyReportConfig    `yaml:"weekly_report"`
	TierCEnrichment TierCEnrichmentConfig `yaml:"tier_c_enrichment"`
}

type ReportingGeminiConfig struct {
	APIKeyEnv string `yaml:"api_key_env"`
	Model     string `yaml:"model"`
	BaseURL   string `yaml:"base_url"`
}

type WeeklyReportConfig struct {
	Enabled      bool                       `yaml:"enabled"`
	Day          string                     `yaml:"day"`
	Time         string                     `yaml:"time"`
	Timezone     string                     `yaml:"timezone"`
	DeviceID     string                     `yaml:"device_id"`
	SensorIDs    []string                   `yaml:"sensor_ids"`
	CustomerName string                     `yaml:"customer_name"`
	SiteName     string                     `yaml:"site_name"`
	Delivery     WeeklyReportDeliveryConfig `yaml:"delivery"`
}

// WeeklyReportDeliveryConfig configures the output channels for completed weekly reports.
type WeeklyReportDeliveryConfig struct {
	File  WeeklyReportFileDeliveryConfig  `yaml:"file"`
	Cloud WeeklyReportCloudDeliveryConfig `yaml:"cloud"`
}

// WeeklyReportFileDeliveryConfig writes a customer-safe JSON artifact to a local directory.
type WeeklyReportFileDeliveryConfig struct {
	Enabled bool   `yaml:"enabled"`
	Path    string `yaml:"path"`
}

// WeeklyReportCloudDeliveryConfig pushes the customer-safe report payload to ori-cloud
// for dashboard persistence. AuthEnv names the environment variable holding the API key.
type WeeklyReportCloudDeliveryConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Endpoint string `yaml:"endpoint"`
	AuthEnv  string `yaml:"auth_env"`
}

type TierCEnrichmentConfig struct {
	Enabled bool `yaml:"enabled"`
}

type SIMConfig struct {
	Enabled   bool   `yaml:"enabled"`
	ModemPath string `yaml:"modem_path"`
}

type FleetConfig struct {
	Enabled  bool   `yaml:"enabled"`
	CloudURL string `yaml:"cloud_url"`
}

// SiteHealthConfig configures the optional site health HTTP export server.
type SiteHealthConfig struct {
	Enabled    bool   `yaml:"enabled"`
	ListenAddr string `yaml:"listen_addr"`
}

// WebhookBridgeConfig configures the optional provider-ingress signing bridge.
// Secret values are resolved from environment variables at runtime.
type WebhookBridgeConfig struct {
	Enabled             bool     `yaml:"enabled"`
	ListenAddr          string   `yaml:"listen_addr"`
	Path                string   `yaml:"path"`
	TargetURL           string   `yaml:"target_url"`
	ProviderSourceCIDRs []string `yaml:"provider_source_cidrs"`
	RuntimeTokenEnv     string   `yaml:"runtime_token_env"`
	HMACSecretEnv       string   `yaml:"hmac_secret_env"`
	RequestTimeoutMS    int      `yaml:"request_timeout_ms"`
	MaxBodyBytes        int64    `yaml:"max_body_bytes"`
}

type fileConfig struct {
	Gateway       fileGatewayConfig       `yaml:"gateway"`
	Provider      fileProviderConfig      `yaml:"provider"`
	Reporting     ReportingConfig         `yaml:"reporting"`
	WebhookBridge fileWebhookBridgeConfig `yaml:"webhook_bridge"`
	SIM           SIMConfig               `yaml:"sim"`
	Fleet         FleetConfig             `yaml:"fleet"`
	SiteHealth    SiteHealthConfig        `yaml:"site_health"`
	Evidence      fileEvidenceConfig      `yaml:"evidence"`
}

type fileGatewayConfig struct {
	BrokerURL          string                  `yaml:"broker_url"`
	DeviceIDs          []string                `yaml:"device_ids"`
	HeartbeatIntervalS *int                    `yaml:"heartbeat_interval_s"`
	Auth               GatewayAuthConfig       `yaml:"auth"`
	Custody            GatewayCustodyConfig    `yaml:"custody"`
	Encryption         GatewayEncryptionConfig `yaml:"encryption"`
}

type fileProviderConfig struct {
	Name      string         `yaml:"name"`
	TimeoutMS *int           `yaml:"timeout_ms"`
	LlamaCpp  LlamaCppConfig `yaml:"llama_cpp"`
	CloudLLM  CloudLLMConfig `yaml:"cloud_llm"`
}

type fileWebhookBridgeConfig struct {
	Enabled             bool     `yaml:"enabled"`
	ListenAddr          string   `yaml:"listen_addr"`
	Path                string   `yaml:"path"`
	TargetURL           string   `yaml:"target_url"`
	ProviderSourceCIDRs []string `yaml:"provider_source_cidrs"`
	RuntimeTokenEnv     string   `yaml:"runtime_token_env"`
	HMACSecretEnv       string   `yaml:"hmac_secret_env"`
	RequestTimeoutMS    *int     `yaml:"request_timeout_ms"`
	MaxBodyBytes        *int64   `yaml:"max_body_bytes"`
}

type fileEvidenceConfig struct {
	Enabled              bool   `yaml:"enabled"`
	QueueDirectory       string `yaml:"queue_directory"`
	ReturnQueueDirectory string `yaml:"return_queue_directory"`
	MaxItems             *int   `yaml:"max_items"`
	MaxBytes             *int64 `yaml:"max_bytes"`
	RetryIntervalS       *int   `yaml:"retry_interval_s"`
	BackoffBaseS         *int   `yaml:"backoff_base_s"`
	BackoffMaxS          *int   `yaml:"backoff_max_s"`
	StoreProbeIntervalS  *int   `yaml:"store_probe_interval_s"`
	EndpointEnv          string `yaml:"endpoint_env"`
	ClientIDEnv          string `yaml:"client_id_env"`
	SecretEnv            string `yaml:"secret_env"`
	AuthorityCAFile      string `yaml:"authority_ca_file"`
	// DeviceCarriage is decoded as supplied; a value that is not a map of
	// strings fails the decode rather than reading as legacy.
	DeviceCarriage map[string]string `yaml:"device_carriage"`
}

// Load reads and validates gateway configuration from path.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}

	var raw fileConfig
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return Config{}, fmt.Errorf("parse config %q: %w", path, err)
	}

	cfg, err := raw.normalize()
	if err != nil {
		return Config{}, fmt.Errorf("validate config %q: %w", path, err)
	}

	return cfg, nil
}

func (f *fileConfig) normalize() (Config, error) {
	cfg := Config{
		Gateway: GatewayConfig{
			BrokerURL: strings.TrimSpace(f.Gateway.BrokerURL),
			DeviceIDs: normalizeGatewayDeviceIDs(f.Gateway.DeviceIDs),
			Auth: GatewayAuthConfig{
				Enabled:                 f.Gateway.Auth.Enabled,
				SharedSecretEnv:         strings.TrimSpace(f.Gateway.Auth.SharedSecretEnv),
				PreviousSharedSecretEnv: strings.TrimSpace(f.Gateway.Auth.PreviousSharedSecretEnv),
			},
			Custody: GatewayCustodyConfig{
				SecretEnv: strings.TrimSpace(f.Gateway.Custody.SecretEnv),
			},
			Encryption: f.Gateway.Encryption,
		},
		Provider: normalizeProviderStrings(ProviderConfig{
			Name:     f.Provider.Name,
			LlamaCpp: f.Provider.LlamaCpp,
			CloudLLM: f.Provider.CloudLLM,
		}),
		Reporting:     normalizeReportingStrings(f.Reporting),
		WebhookBridge: normalizeWebhookBridge(f.WebhookBridge),
		SIM:           f.SIM,
		Fleet:         f.Fleet,
		SiteHealth:    normalizeSiteHealth(f.SiteHealth),
		Evidence:      normalizeEvidence(f.Evidence),
	}

	if cfg.Gateway.BrokerURL == "" {
		return Config{}, fmt.Errorf("gateway.broker_url must not be empty")
	}
	if len(cfg.Gateway.DeviceIDs) == 0 {
		return Config{}, fmt.Errorf("gateway.device_ids must include at least one runtime device")
	}
	for _, deviceID := range cfg.Gateway.DeviceIDs {
		if err := validateGatewayDeviceID(deviceID); err != nil {
			return Config{}, fmt.Errorf("gateway.device_ids: %w", err)
		}
	}
	if cfg.Gateway.Auth.SharedSecretEnv == "" {
		cfg.Gateway.Auth.SharedSecretEnv = DefaultGatewayAuthSecretEnv
	}
	if strings.ContainsAny(cfg.Gateway.Auth.SharedSecretEnv, " \t\r\n=") {
		return Config{}, fmt.Errorf("gateway.auth.shared_secret_env must be an environment variable name")
	}
	if cfg.Gateway.Auth.PreviousSharedSecretEnv != "" && strings.ContainsAny(cfg.Gateway.Auth.PreviousSharedSecretEnv, " \t\r\n=") {
		return Config{}, fmt.Errorf("gateway.auth.previous_shared_secret_env must be an environment variable name")
	}
	// Separation is checked on the names here and on the resolved bytes where
	// the secrets are read. A name check alone would pass two variables holding
	// one value, which is the reuse this exists to prevent.
	if strings.ContainsAny(cfg.Gateway.Custody.SecretEnv, " \t\r\n=") {
		return Config{}, fmt.Errorf("gateway.custody.secret_env must be an environment variable name")
	}
	if cfg.Gateway.Custody.SecretEnv != "" {
		if cfg.Gateway.Custody.SecretEnv == cfg.Gateway.Auth.SharedSecretEnv {
			return Config{}, fmt.Errorf("gateway.custody.secret_env must differ from gateway.auth.shared_secret_env")
		}
		if cfg.Gateway.Custody.SecretEnv == cfg.Gateway.Auth.PreviousSharedSecretEnv {
			return Config{}, fmt.Errorf("gateway.custody.secret_env must differ from gateway.auth.previous_shared_secret_env")
		}
	}
	if cfg.Gateway.Encryption.Enabled && !cfg.Gateway.Auth.Enabled {
		return Config{}, fmt.Errorf("gateway.encryption.enabled requires gateway.auth.enabled")
	}

	if f.Gateway.HeartbeatIntervalS == nil {
		cfg.Gateway.HeartbeatIntervalS = DefaultHeartbeatIntervalS
	} else if *f.Gateway.HeartbeatIntervalS <= 0 {
		return Config{}, fmt.Errorf("gateway.heartbeat_interval_s must be positive")
	} else {
		cfg.Gateway.HeartbeatIntervalS = *f.Gateway.HeartbeatIntervalS
	}

	if f.Provider.TimeoutMS == nil {
		cfg.Provider.TimeoutMS = DefaultProviderTimeoutMS
	} else if *f.Provider.TimeoutMS <= 0 {
		return Config{}, fmt.Errorf("provider.timeout_ms must be positive")
	} else {
		cfg.Provider.TimeoutMS = *f.Provider.TimeoutMS
	}

	if cfg.Provider.Name == "" {
		return Config{}, fmt.Errorf("provider.name must not be empty")
	}
	if !isKnownProvider(cfg.Provider.Name) {
		return Config{}, fmt.Errorf(
			"provider.name %q is unknown (allowed: echo, llama_cpp, cloud_llm)",
			cfg.Provider.Name,
		)
	}

	if err := validateProvider(cfg.Provider); err != nil {
		return Config{}, err
	}

	if err := validateReporting(cfg.Reporting); err != nil {
		return Config{}, err
	}

	if err := validateWebhookBridge(cfg.WebhookBridge); err != nil {
		return Config{}, err
	}

	if cfg.SIM.Enabled && strings.TrimSpace(cfg.SIM.ModemPath) == "" {
		return Config{}, fmt.Errorf("sim.modem_path must not be empty when sim.enabled is true")
	}

	if cfg.Fleet.Enabled && strings.TrimSpace(cfg.Fleet.CloudURL) == "" {
		return Config{}, fmt.Errorf("fleet.cloud_url must not be empty when fleet.enabled is true")
	}

	if err := validateSiteHealth(cfg.SiteHealth); err != nil {
		return Config{}, err
	}
	if err := validateEvidence(&cfg.Evidence, cfg.Gateway.DeviceIDs); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func normalizeEvidence(raw fileEvidenceConfig) EvidenceConfig {
	maxItems := DefaultEvidenceMaxItems
	if raw.MaxItems != nil {
		maxItems = *raw.MaxItems
	}
	maxBytes := DefaultEvidenceMaxBytes
	if raw.MaxBytes != nil {
		maxBytes = *raw.MaxBytes
	}
	retry := DefaultEvidenceRetryS
	if raw.RetryIntervalS != nil {
		retry = *raw.RetryIntervalS
	}
	// Unset, the base is the delivery interval and the bound is five minutes
	// or the base, whichever is longer; set, they are validated as given.
	base := retry
	if raw.BackoffBaseS != nil {
		base = *raw.BackoffBaseS
	}
	bound := max(DefaultEvidenceBackoffMaxS, base)
	if raw.BackoffMaxS != nil {
		bound = *raw.BackoffMaxS
	}
	probe := DefaultEvidenceStoreProbeIntervalS
	if raw.StoreProbeIntervalS != nil {
		probe = *raw.StoreProbeIntervalS
	}
	return EvidenceConfig{
		StoreProbeIntervalS:  probe,
		BackoffBaseS:         base,
		BackoffMaxS:          bound,
		Enabled:              raw.Enabled,
		QueueDirectory:       strings.TrimSpace(raw.QueueDirectory),
		ReturnQueueDirectory: strings.TrimSpace(raw.ReturnQueueDirectory),
		MaxItems:             maxItems,
		MaxBytes:             maxBytes,
		RetryIntervalS:       retry,
		EndpointEnv:          strings.TrimSpace(raw.EndpointEnv),
		ClientIDEnv:          strings.TrimSpace(raw.ClientIDEnv),
		SecretEnv:            strings.TrimSpace(raw.SecretEnv),
		AuthorityCAFile:      raw.AuthorityCAFile,
		DeviceCarriage:       raw.DeviceCarriage,
	}
}

// validateEvidence checks the evidence courier. Two rule sets apply
// (gateway-config/v2): the courier's own keys whenever it is enabled, and the
// versioned carriage's rules only when at least one configured device declares
// gateway-evidence-carriage/v1. A site that declares nothing keeps exactly the
// courier's own validation, so an upgrade refuses no configuration it accepted.
// With the courier enabled, a supplied declaration is validated even when it
// declares nothing: an invalid one is refused, never read as legacy. A disabled
// courier validates none of its keys, the declaration included.
func validateEvidence(cfg *EvidenceConfig, deviceIDs []string) error {
	if !cfg.Enabled {
		cfg.DeviceCarriage = nil
		return nil
	}
	if !filepath.IsAbs(cfg.QueueDirectory) || !filepath.IsAbs(cfg.ReturnQueueDirectory) {
		return fmt.Errorf("evidence queue directories must be absolute")
	}
	if filepath.Clean(cfg.QueueDirectory) == filepath.Clean(cfg.ReturnQueueDirectory) {
		return fmt.Errorf("evidence outbound and return queues must use distinct directories")
	}
	// Its contents are verified when the courier starts; here only its form.
	if cfg.AuthorityCAFile != "" && (!filepath.IsAbs(cfg.AuthorityCAFile) || filepath.Clean(cfg.AuthorityCAFile) != cfg.AuthorityCAFile || strings.TrimSpace(cfg.AuthorityCAFile) != cfg.AuthorityCAFile) {
		return fmt.Errorf("evidence.authority_ca_file must be an absolute, clean path")
	}
	if cfg.MaxItems <= 0 || cfg.MaxBytes <= 0 || cfg.RetryIntervalS <= 0 {
		return fmt.Errorf("evidence queue bounds and retry interval must be positive")
	}
	for field, value := range map[string]string{
		"evidence.endpoint_env":  cfg.EndpointEnv,
		"evidence.client_id_env": cfg.ClientIDEnv,
		"evidence.secret_env":    cfg.SecretEnv,
	} {
		if err := validateEnvVarName(field, value); err != nil {
			return err
		}
	}
	carriage, err := resolveDeviceCarriage(cfg.DeviceCarriage, deviceIDs)
	if err != nil {
		return err
	}
	cfg.DeviceCarriage = carriage
	if !cfg.DeclaresVersionedCarriage() {
		return nil
	}
	// The versioned carriage's rules, refused at load before any lane opens.
	for _, deviceID := range deviceIDs {
		if carriage[deviceID] != CarriageEvidenceCarriageV1 {
			continue
		}
		if err := validateEvidenceDeviceID(deviceID); err != nil {
			return err
		}
	}
	if cfg.BackoffMaxS < cfg.BackoffBaseS || cfg.BackoffBaseS < cfg.RetryIntervalS {
		return fmt.Errorf("evidence back-off must satisfy backoff_max_s >= backoff_base_s >= retry_interval_s (got %d, %d, %d)",
			cfg.BackoffMaxS, cfg.BackoffBaseS, cfg.RetryIntervalS)
	}
	// Capacity is shared across every configured device, legacy lanes
	// included: each consumes its share.
	devices := len(deviceIDs)
	if devices > 0 && (cfg.MaxItems/devices < courier.MinDeviceShareItems || cfg.MaxBytes/int64(devices) < courier.MinDeviceShareBytes) {
		return fmt.Errorf("evidence.max_items and evidence.max_bytes are shared equally across %d devices; each share must be at least %d items and %d bytes (a registration reserve and one evidence record)",
			devices, courier.MinDeviceShareItems, courier.MinDeviceShareBytes)
	}
	if cfg.StoreProbeIntervalS < MinEvidenceStoreProbeIntervalS || cfg.StoreProbeIntervalS > MaxEvidenceStoreProbeIntervalS {
		return fmt.Errorf("evidence.store_probe_interval_s must be %d through %d seconds (got %d)",
			MinEvidenceStoreProbeIntervalS, MaxEvidenceStoreProbeIntervalS, cfg.StoreProbeIntervalS)
	}
	return nil
}

// resolveDeviceCarriage validates a supplied evidence.device_carriage and
// returns every configured device's carriage, gateway-api/v1 where the map does
// not name it. An absent or empty map declares nothing.
func resolveDeviceCarriage(declared map[string]string, deviceIDs []string) (map[string]string, error) {
	configured := make(map[string]bool, len(deviceIDs))
	for _, deviceID := range deviceIDs {
		configured[deviceID] = true
	}
	for deviceID, carriage := range declared {
		if !configured[deviceID] {
			return nil, fmt.Errorf("evidence.device_carriage names device_id %q, which gateway.device_ids does not configure", deviceID)
		}
		if carriage != CarriageGatewayAPIV1 && carriage != CarriageEvidenceCarriageV1 {
			return nil, fmt.Errorf("evidence.device_carriage[%q] must be %q or %q (got %q)",
				deviceID, CarriageGatewayAPIV1, CarriageEvidenceCarriageV1, carriage)
		}
	}
	out := make(map[string]string, len(deviceIDs))
	for _, deviceID := range deviceIDs {
		out[deviceID] = CarriageGatewayAPIV1
		if carriage, ok := declared[deviceID]; ok {
			out[deviceID] = carriage
		}
	}
	return out, nil
}

func normalizeSiteHealth(cfg SiteHealthConfig) SiteHealthConfig {
	if !cfg.Enabled {
		return cfg
	}
	return SiteHealthConfig{
		Enabled:    true,
		ListenAddr: defaultIfBlank(cfg.ListenAddr, DefaultSiteHealthListenAddr),
	}
}

func validateSiteHealth(cfg SiteHealthConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if _, _, err := net.SplitHostPort(cfg.ListenAddr); err != nil {
		return fmt.Errorf("site_health.listen_addr must be host:port: %w", err)
	}
	return nil
}

func normalizeGatewayDeviceIDs(deviceIDs []string) []string {
	out := make([]string, 0, len(deviceIDs))
	seen := map[string]bool{}
	for _, deviceID := range deviceIDs {
		if seen[deviceID] {
			continue
		}
		seen[deviceID] = true
		out = append(out, deviceID)
	}
	return out
}

func validateGatewayDeviceID(deviceID string) error {
	if deviceID == "" {
		return fmt.Errorf("device_id must not be empty")
	}
	if strings.TrimSpace(deviceID) != deviceID {
		return fmt.Errorf("device_id %q must not contain leading or trailing whitespace", deviceID)
	}
	if strings.ContainsAny(deviceID, "/+#|") {
		return fmt.Errorf("device_id %q must not contain MQTT separators, wildcards, or auth delimiters", deviceID)
	}
	return nil
}

// validateEvidenceDeviceID holds a device that declares
// gateway-evidence-carriage/v1 to the evidence routing domain.
func validateEvidenceDeviceID(deviceID string) error {
	if !contracts.ValidEvidenceRoutingDeviceID(deviceID) {
		return fmt.Errorf("gateway.device_ids: device_id %q declares gateway-evidence-carriage/v1, so it must be 1 to 128 Unicode characters with no control character, whitespace, \"/\", \"+\" or \"#\"", deviceID)
	}
	return nil
}

func isKnownProvider(name string) bool {
	switch name {
	case ProviderEcho, ProviderLlamaCpp, ProviderCloudLLM:
		return true
	default:
		return false
	}
}

func normalizeProviderStrings(provider ProviderConfig) ProviderConfig {
	provider.Name = strings.TrimSpace(provider.Name)
	provider.LlamaCpp.URL = strings.TrimSpace(provider.LlamaCpp.URL)
	provider.LlamaCpp.Model = strings.TrimSpace(provider.LlamaCpp.Model)
	provider.CloudLLM.Vendor = strings.TrimSpace(provider.CloudLLM.Vendor)
	provider.CloudLLM.APIKeyEnv = strings.TrimSpace(provider.CloudLLM.APIKeyEnv)
	provider.CloudLLM.Model = strings.TrimSpace(provider.CloudLLM.Model)
	provider.CloudLLM.BaseURL = strings.TrimSpace(provider.CloudLLM.BaseURL)
	return provider
}

func normalizeWebhookBridge(raw fileWebhookBridgeConfig) WebhookBridgeConfig {
	requestTimeoutMS := DefaultWebhookBridgeRequestTimeoutMS
	if raw.RequestTimeoutMS != nil {
		requestTimeoutMS = *raw.RequestTimeoutMS
	}
	maxBodyBytes := int64(DefaultWebhookBridgeMaxBodyBytes)
	if raw.MaxBodyBytes != nil {
		maxBodyBytes = *raw.MaxBodyBytes
	}
	cidrs := make([]string, 0, len(raw.ProviderSourceCIDRs))
	for _, cidr := range raw.ProviderSourceCIDRs {
		trimmed := strings.TrimSpace(cidr)
		if trimmed != "" {
			cidrs = append(cidrs, trimmed)
		}
	}
	return WebhookBridgeConfig{
		Enabled:             raw.Enabled,
		ListenAddr:          defaultIfBlank(raw.ListenAddr, DefaultWebhookBridgeListenAddr),
		Path:                defaultIfBlank(raw.Path, DefaultWebhookBridgePath),
		TargetURL:           strings.TrimSpace(raw.TargetURL),
		ProviderSourceCIDRs: cidrs,
		RuntimeTokenEnv:     strings.TrimSpace(raw.RuntimeTokenEnv),
		HMACSecretEnv:       strings.TrimSpace(raw.HMACSecretEnv),
		RequestTimeoutMS:    requestTimeoutMS,
		MaxBodyBytes:        maxBodyBytes,
	}
}

func defaultIfBlank(value string, fallback string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}

func validateWebhookBridge(cfg WebhookBridgeConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if _, _, err := net.SplitHostPort(cfg.ListenAddr); err != nil {
		return fmt.Errorf("webhook_bridge.listen_addr must be host:port: %w", err)
	}
	if !strings.HasPrefix(cfg.Path, "/") {
		return fmt.Errorf("webhook_bridge.path must start with /")
	}
	if cfg.TargetURL == "" {
		return fmt.Errorf("webhook_bridge.target_url must not be empty when webhook_bridge.enabled is true")
	}
	target, err := url.Parse(cfg.TargetURL)
	if err != nil || target.Scheme == "" || target.Host == "" {
		return fmt.Errorf("webhook_bridge.target_url must be an absolute http(s) URL")
	}
	if target.Scheme != "http" && target.Scheme != "https" {
		return fmt.Errorf("webhook_bridge.target_url must use http or https")
	}
	if err := validateEnvVarName("webhook_bridge.runtime_token_env", cfg.RuntimeTokenEnv); err != nil {
		return err
	}
	if err := validateEnvVarName("webhook_bridge.hmac_secret_env", cfg.HMACSecretEnv); err != nil {
		return err
	}
	if cfg.RequestTimeoutMS <= 0 {
		return fmt.Errorf("webhook_bridge.request_timeout_ms must be positive")
	}
	if cfg.MaxBodyBytes <= 0 || cfg.MaxBodyBytes > 1<<20 {
		return fmt.Errorf("webhook_bridge.max_body_bytes must be between 1 and 1048576")
	}
	for _, cidr := range cfg.ProviderSourceCIDRs {
		prefix, err := netip.ParsePrefix(cidr)
		if err != nil {
			return fmt.Errorf("webhook_bridge.provider_source_cidrs contains invalid CIDR %q: %w", cidr, err)
		}
		if err := validateProviderCIDR(prefix, cidr); err != nil {
			return err
		}
	}
	if !listenAddrIsLoopback(cfg.ListenAddr) && len(cfg.ProviderSourceCIDRs) == 0 {
		return fmt.Errorf("webhook_bridge.provider_source_cidrs must not be empty for non-loopback listen_addr")
	}
	return nil
}

func validateProviderCIDR(prefix netip.Prefix, raw string) error {
	bits := prefix.Bits()
	addr := prefix.Addr()
	if bits == 0 {
		return fmt.Errorf("webhook_bridge.provider_source_cidrs must not contain catch-all CIDR %q", raw)
	}
	if addr.Is4() && bits < 8 {
		return fmt.Errorf("webhook_bridge.provider_source_cidrs CIDR %q is too broad; IPv4 prefixes must be /8 or narrower", raw)
	}
	if addr.Is6() && bits < 32 {
		return fmt.Errorf("webhook_bridge.provider_source_cidrs CIDR %q is too broad; IPv6 prefixes must be /32 or narrower", raw)
	}
	return nil
}

func validateEnvVarName(field string, value string) error {
	if value == "" {
		return fmt.Errorf("%s must be an environment variable name", field)
	}
	if strings.ContainsAny(value, " \t\r\n=") {
		return fmt.Errorf("%s must be an environment variable name", field)
	}
	return nil
}

func listenAddrIsLoopback(listenAddr string) bool {
	host, _, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	return addr.IsLoopback()
}

func validateProvider(provider ProviderConfig) error {
	if provider.Name == ProviderLlamaCpp {
		if provider.LlamaCpp.URL == "" {
			return fmt.Errorf("provider.llama_cpp.url must not be empty when provider.name is llama_cpp")
		}
		return nil
	}
	if provider.Name != ProviderCloudLLM {
		return nil
	}

	if provider.CloudLLM.Vendor == "" {
		return fmt.Errorf("provider.cloud_llm.vendor must not be empty when provider.name is cloud_llm")
	}
	if !isKnownCloudVendor(provider.CloudLLM.Vendor) {
		return fmt.Errorf(
			"provider.cloud_llm.vendor %q is unknown (allowed: claude, openai, gemini, deepseek, openai_compatible)",
			provider.CloudLLM.Vendor,
		)
	}
	if provider.CloudLLM.APIKeyEnv == "" {
		return fmt.Errorf("provider.cloud_llm.api_key_env must not be empty when provider.name is cloud_llm")
	}
	if strings.ContainsAny(provider.CloudLLM.APIKeyEnv, " \t\r\n") {
		return fmt.Errorf("provider.cloud_llm.api_key_env must be an environment variable name")
	}
	if provider.CloudLLM.Model == "" {
		return fmt.Errorf("provider.cloud_llm.model must not be empty when provider.name is cloud_llm")
	}
	if provider.CloudLLM.Vendor == CloudVendorOpenAICompatible && provider.CloudLLM.BaseURL == "" {
		return fmt.Errorf("provider.cloud_llm.base_url must not be empty when vendor is openai_compatible")
	}

	return nil
}

func isKnownCloudVendor(vendor string) bool {
	switch vendor {
	case CloudVendorClaude, CloudVendorOpenAI, CloudVendorGemini, CloudVendorDeepSeek, CloudVendorOpenAICompatible:
		return true
	default:
		return false
	}
}

func normalizeReportingStrings(reporting ReportingConfig) ReportingConfig {
	reporting.Provider = strings.TrimSpace(reporting.Provider)
	reporting.Gemini.APIKeyEnv = strings.TrimSpace(reporting.Gemini.APIKeyEnv)
	reporting.Gemini.Model = strings.TrimSpace(reporting.Gemini.Model)
	reporting.Gemini.BaseURL = strings.TrimSpace(reporting.Gemini.BaseURL)
	reporting.WeeklyReport.Day = strings.TrimSpace(reporting.WeeklyReport.Day)
	reporting.WeeklyReport.Time = strings.TrimSpace(reporting.WeeklyReport.Time)
	reporting.WeeklyReport.Timezone = strings.TrimSpace(reporting.WeeklyReport.Timezone)
	reporting.WeeklyReport.DeviceID = strings.TrimSpace(reporting.WeeklyReport.DeviceID)
	reporting.WeeklyReport.CustomerName = strings.TrimSpace(reporting.WeeklyReport.CustomerName)
	reporting.WeeklyReport.SiteName = strings.TrimSpace(reporting.WeeklyReport.SiteName)
	reporting.WeeklyReport.Delivery.File.Path = strings.TrimSpace(reporting.WeeklyReport.Delivery.File.Path)
	reporting.WeeklyReport.Delivery.Cloud.Endpoint = strings.TrimSpace(reporting.WeeklyReport.Delivery.Cloud.Endpoint)
	reporting.WeeklyReport.Delivery.Cloud.AuthEnv = strings.TrimSpace(reporting.WeeklyReport.Delivery.Cloud.AuthEnv)
	for i := range reporting.WeeklyReport.SensorIDs {
		reporting.WeeklyReport.SensorIDs[i] = strings.TrimSpace(reporting.WeeklyReport.SensorIDs[i])
	}
	return reporting
}

func validateReporting(reporting ReportingConfig) error {
	if reporting.Provider != "" && !isKnownReportingProvider(reporting.Provider) {
		return fmt.Errorf(
			"reporting.provider %q is unknown (allowed: gemini)",
			reporting.Provider,
		)
	}

	if !reporting.WeeklyReport.Enabled && !reporting.TierCEnrichment.Enabled {
		return nil
	}

	if reporting.Provider == "" {
		return fmt.Errorf("reporting.provider must not be empty when reporting features are enabled")
	}
	if !isKnownReportingProvider(reporting.Provider) {
		return fmt.Errorf(
			"reporting.provider %q is unknown (allowed: gemini)",
			reporting.Provider,
		)
	}

	if reporting.Provider == ReportingProviderGemini {
		if reporting.Gemini.APIKeyEnv == "" {
			return fmt.Errorf("reporting.gemini.api_key_env must not be empty when reporting features are enabled")
		}
		if strings.ContainsAny(reporting.Gemini.APIKeyEnv, " \t\r\n") {
			return fmt.Errorf("reporting.gemini.api_key_env must be an environment variable name")
		}
		if reporting.Gemini.Model == "" {
			return fmt.Errorf("reporting.gemini.model must not be empty when reporting features are enabled")
		}
	}

	if reporting.WeeklyReport.Enabled {
		if !isValidWeekday(reporting.WeeklyReport.Day) {
			return fmt.Errorf("reporting.weekly_report.day must be a weekday name")
		}
		if _, err := time.Parse("15:04", reporting.WeeklyReport.Time); err != nil {
			return fmt.Errorf("reporting.weekly_report.time must use HH:MM 24-hour format")
		}
		if reporting.WeeklyReport.Timezone == "" {
			return fmt.Errorf("reporting.weekly_report.timezone must not be empty")
		}
		if _, err := time.LoadLocation(reporting.WeeklyReport.Timezone); err != nil {
			return fmt.Errorf("reporting.weekly_report.timezone %q is invalid: %w", reporting.WeeklyReport.Timezone, err)
		}
		if reporting.WeeklyReport.DeviceID == "" {
			return fmt.Errorf("reporting.weekly_report.device_id must not be empty")
		}
		if strings.ContainsAny(reporting.WeeklyReport.DeviceID, "/+#") {
			return fmt.Errorf("reporting.weekly_report.device_id must not contain MQTT topic separators or wildcards")
		}
		if len(reporting.WeeklyReport.SensorIDs) == 0 {
			return fmt.Errorf("reporting.weekly_report.sensor_ids must not be empty")
		}
		if slices.Contains(reporting.WeeklyReport.SensorIDs, "") {
			return fmt.Errorf("reporting.weekly_report.sensor_ids must not contain empty values")
		}
		if reporting.WeeklyReport.Delivery.File.Enabled {
			path := reporting.WeeklyReport.Delivery.File.Path
			if path == "" {
				return fmt.Errorf("reporting.weekly_report.delivery.file.path must not be empty when file delivery is enabled")
			}
			if !filepath.IsAbs(path) {
				return fmt.Errorf("reporting.weekly_report.delivery.file.path must be an absolute path")
			}
			if slices.Contains(strings.Split(path, "/"), "..") {
				return fmt.Errorf("reporting.weekly_report.delivery.file.path must not contain path traversal segments")
			}
		}
		if reporting.WeeklyReport.Delivery.Cloud.Enabled {
			if reporting.WeeklyReport.Delivery.Cloud.Endpoint == "" {
				return fmt.Errorf("reporting.weekly_report.delivery.cloud.endpoint must not be empty when cloud delivery is enabled")
			}
			endpoint, err := url.Parse(reporting.WeeklyReport.Delivery.Cloud.Endpoint)
			if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" {
				return fmt.Errorf("reporting.weekly_report.delivery.cloud.endpoint must be an absolute https URL")
			}
			if err := validateEnvVarName("reporting.weekly_report.delivery.cloud.auth_env", reporting.WeeklyReport.Delivery.Cloud.AuthEnv); err != nil {
				return err
			}
		}
	}

	return nil
}

func isKnownReportingProvider(name string) bool {
	switch name {
	case ReportingProviderGemini:
		return true
	default:
		return false
	}
}

func isValidWeekday(day string) bool {
	switch strings.ToLower(day) {
	case "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday":
		return true
	default:
		return false
	}
}
