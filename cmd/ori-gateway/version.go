// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"io"
	"runtime"
	"runtime/debug"
)

// Build provenance. These are overridable at release build time via
//
//	-ldflags "-X main.version=vX.Y.Z -X main.commit=<sha> -X main.buildDate=<rfc3339>"
//
// but they are deliberately not required: an ordinary `go build` still yields
// truthful provenance from the embedded VCS metadata below. The version stays
// "dev" until a supported release build injects a concrete value — a tag is a
// distribution label, not a correctness prerequisite.
var (
	version   = "dev"
	commit    = ""
	buildDate = ""
)

type buildProvenance struct {
	Version   string
	Commit    string
	BuildDate string
	Modified  bool
	GoVersion string
}

// gatewayProvenance resolves build provenance, preferring ldflags-injected
// values and falling back to the module's embedded VCS metadata so a plain
// build still reports the exact source commit it was built from.
func gatewayProvenance(info *debug.BuildInfo, ok bool) buildProvenance {
	p := buildProvenance{
		Version:   version,
		Commit:    commit,
		BuildDate: buildDate,
		GoVersion: runtime.Version(),
	}
	if ok && info != nil {
		if p.Version == "dev" && info.Main.Version != "" && info.Main.Version != "(devel)" {
			p.Version = info.Main.Version
		}
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				if p.Commit == "" {
					p.Commit = setting.Value
				}
			case "vcs.time":
				if p.BuildDate == "" {
					p.BuildDate = setting.Value
				}
			case "vcs.modified":
				p.Modified = setting.Value == "true"
			}
		}
	}
	if p.Commit == "" {
		p.Commit = "unknown"
	}
	if p.BuildDate == "" {
		p.BuildDate = "unknown"
	}
	return p
}

func writeVersion(w io.Writer, p buildProvenance) {
	commitLine := p.Commit
	if p.Modified {
		commitLine += " (modified)"
	}
	fmt.Fprintf(w, "ori-gateway %s\n", p.Version)
	fmt.Fprintf(w, "commit: %s\n", commitLine)
	fmt.Fprintf(w, "built:  %s\n", p.BuildDate)
	fmt.Fprintf(w, "go:     %s\n", p.GoVersion)
}
