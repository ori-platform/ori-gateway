// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/specvectors"
)

type memberCorpus struct {
	Reasons  []string            `json:"reasons"`
	Families map[string][]string `json:"families"`
	Cases    []struct {
		Name       string  `json:"name"`
		PayloadHex string  `json:"payload_hex"`
		Family     *string `json:"family"`
		Expected   string  `json:"expected"`
	} `json:"cases"`
}

func loadMemberCorpus(t *testing.T) memberCorpus {
	t.Helper()
	raw, err := specvectors.Read("gateway-api/vectors/payload-members-v2.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus memberCorpus
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	return corpus
}

// TestDefinedMembersMatchTheCorpus holds the gateway's member table equal to
// the corpus's list of every topic's defined members.
func TestDefinedMembersMatchTheCorpus(t *testing.T) {
	corpus := loadMemberCorpus(t)
	families := map[string]string{}
	for family := range corpus.Families {
		families[family] = family
	}
	checkLANPinsExist(t, lanFamilyPins, families)
	for family := range contracts.PayloadMaxima {
		if _, ok := families[family]; !ok {
			t.Errorf("%s: in the gateway's table, not in the corpus", family)
		}
	}
	for family, want := range corpus.Families {
		observed := ""
		if _, ok := contracts.PayloadMaxima[family]; !ok {
			observed = "not in the gateway's member table"
		}
		if checkLANPin(t, lanFamilyPins, family, observed) {
			continue
		}
		got := slices.Clone(contracts.DefinedMembers(family))
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Errorf("%s: gateway members %v, corpus %v", family, got, want)
		}
	}
	reasons := []string{contracts.ReasonInvalidUnicode, contracts.ReasonMalformedJSON, contracts.ReasonNestingTooDeep,
		contracts.ReasonTrailingData, contracts.ReasonDuplicateMember, contracts.ReasonCaseVariantMember, contracts.ReasonNotAnObject}
	if !slices.Equal(reasons, corpus.Reasons) {
		t.Fatalf("reasons %v, corpus %v", reasons, corpus.Reasons)
	}
}

// TestPayloadMemberVectorCases drives every corpus case. A case on a family
// the gateway receives goes through the real message wrapper: an admitted
// payload reaches its handler, a refused one never does and is logged with
// its reason. Every other case goes through the same admission with its
// family's members, or none.
func TestPayloadMemberVectorCases(t *testing.T) {
	corpus := loadMemberCorpus(t)
	received := map[string]bool{}
	for _, family := range contracts.ReceivedFamilies {
		received[family] = true
	}
	names := map[string]string{}
	for _, c := range corpus.Cases {
		if c.Family != nil {
			names[c.Name] = *c.Family
		} else {
			names[c.Name] = ""
		}
	}
	checkLANPinsExist(t, lanMemberCasePins, names)
	throughWrapper := 0
	for _, c := range corpus.Cases {
		payload, err := hex.DecodeString(c.PayloadHex)
		if err != nil {
			t.Fatalf("%s: %v", c.Name, err)
		}
		if c.Family != nil && received[*c.Family] {
			throughWrapper++
			r := newIngressRig(t)
			handled := r.subscription(t, ingressTopics[*c.Family])(payload)
			if handled != (c.Expected == "accept") {
				t.Errorf("%s: handled=%v, want %s", c.Name, handled, c.Expected)
				continue
			}
			if c.Expected != "accept" {
				assertLogged(t, r, "reason="+c.Expected)
			}
			continue
		}
		var members []string
		if c.Family != nil {
			members = contracts.DefinedMembers(*c.Family)
		}
		got := "accept"
		var refused *contracts.MemberRuleError
		if err := contracts.AdmitPayloadWithMembers(members, payload); errors.As(err, &refused) {
			got = refused.Reason
		} else if err != nil {
			t.Fatalf("%s: %v", c.Name, err)
		}
		observed := ""
		if got != c.Expected {
			observed = got + ", want " + c.Expected
		}
		checkLANPin(t, lanMemberCasePins, c.Name, observed)
	}
	if throughWrapper == 0 {
		t.Fatal("no corpus case reached the message wrapper")
	}
}

// assertLogged requires the log to hold want now: the first refusal for a
// family and reason is written synchronously.
func assertLogged(t *testing.T, r *ingressRig, want string) {
	t.Helper()
	r.client.flushLogs(t)
	if !strings.Contains(r.logs.String(), want) {
		t.Fatalf("log lacks %q:\n%s", want, r.logs.String())
	}
}
