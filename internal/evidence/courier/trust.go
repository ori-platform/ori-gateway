// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"os"
)

// maxAuthorityTrustBytes bounds the CA bundle read at startup.
const maxAuthorityTrustBytes = 1 << 20

// LoadAuthorityTrust reads the CA bundle the courier trusts for the evidence
// authority, in place of the system trust store.
//
// Whoever can change this file decides whom the gateway believes it is
// delivering evidence to, so the file must be a regular file that no group or
// other user can write. It holds only CA certificates: several during a
// rotation, and never a private key or a leaf.
func LoadAuthorityTrust(path string) (*x509.CertPool, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("evidence: authority CA bundle: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("evidence: authority CA bundle must be a regular file")
	}
	if info.Mode().Perm()&0o022 != 0 {
		return nil, fmt.Errorf("evidence: authority CA bundle must not be writable by group or others")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("evidence: authority CA bundle: %w", err)
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("evidence: authority CA bundle: %w", err)
	}
	if !os.SameFile(info, opened) {
		return nil, fmt.Errorf("evidence: authority CA bundle changed while it was opened")
	}
	data, err := io.ReadAll(io.LimitReader(file, maxAuthorityTrustBytes+1))
	if err != nil {
		return nil, fmt.Errorf("evidence: authority CA bundle: %w", err)
	}
	if len(data) > maxAuthorityTrustBytes {
		return nil, fmt.Errorf("evidence: authority CA bundle exceeds %d bytes", maxAuthorityTrustBytes)
	}
	pool := x509.NewCertPool()
	count := 0
	rest := data
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			return nil, fmt.Errorf("evidence: authority CA bundle holds a %q block; only CA certificates belong there", block.Type)
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("evidence: authority CA bundle: certificate %d: %w", count+1, err)
		}
		if !cert.BasicConstraintsValid || !cert.IsCA {
			return nil, fmt.Errorf("evidence: authority CA bundle: certificate %d is not a CA", count+1)
		}
		pool.AddCert(cert)
		count++
	}
	if len(bytes.TrimSpace(rest)) != 0 {
		return nil, fmt.Errorf("evidence: authority CA bundle carries data that is not a PEM certificate")
	}
	if count == 0 {
		return nil, fmt.Errorf("evidence: authority CA bundle holds no certificate")
	}
	return pool, nil
}
