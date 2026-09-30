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
	"path/filepath"
)

// maxAuthorityTrustBytes bounds the CA bundle read at startup.
const maxAuthorityTrustBytes = 1 << 20

// LoadAuthorityTrust reads the CA bundle the courier trusts for the evidence
// authority, in place of the system trust store.
//
// Whoever can change this file, or any directory above it, decides whom the
// gateway believes it is delivering evidence to. So the path must be absolute
// and clean with no symlink in any component, and the file and every directory
// above it must be owned by root or by trustedOwner and writable by no group
// or other user. The gateway passes root (0) as trustedOwner. The file holds
// only CA certificates: several during a rotation, never a private key or a
// leaf.
func LoadAuthorityTrust(path string, trustedOwner int) (*x509.CertPool, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, fmt.Errorf("evidence: authority CA bundle path must be absolute and clean")
	}
	info, err := checkTrustPath(path, trustedOwner)
	if err != nil {
		return nil, err
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
	return parseAuthorityTrust(data)
}

// checkTrustPath checks the bundle and every directory above it, and returns
// the bundle's own information.
func checkTrustPath(path string, trustedOwner int) (os.FileInfo, error) {
	var bundle os.FileInfo
	for component := path; ; component = filepath.Dir(component) {
		info, err := os.Lstat(component)
		if err != nil {
			return nil, fmt.Errorf("evidence: authority CA bundle: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("evidence: authority CA bundle path component %s is a symlink", component)
		}
		if bundle == nil {
			if !info.Mode().IsRegular() {
				return nil, fmt.Errorf("evidence: authority CA bundle must be a regular file")
			}
			bundle = info
		}
		if info.Mode().Perm()&0o022 != 0 {
			return nil, fmt.Errorf("evidence: authority CA bundle path component %s is writable by group or others", component)
		}
		owner, ok := fileOwner(info)
		if !ok {
			return nil, fmt.Errorf("evidence: authority CA bundle owner cannot be determined on this platform")
		}
		if owner != 0 && owner != trustedOwner {
			return nil, fmt.Errorf("evidence: authority CA bundle path component %s is owned by an untrusted user", component)
		}
		if parent := filepath.Dir(component); parent == component {
			return bundle, nil
		}
	}
}

func parseAuthorityTrust(data []byte) (*x509.CertPool, error) {
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
