// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"sync"

	"github.com/ori-platform/ori-gateway/internal/contracts"
)

type RetryPolicy struct {
	TimeoutMS  int64
	MaxRetries int
}

func (p RetryPolicy) TotalAttempts() int {
	return p.MaxRetries + 1
}

type Session struct {
	Request  contracts.ReasoningRequest
	Policy   RetryPolicy
	Started  int64
	Attempts int
}

func New(req contracts.ReasoningRequest, policy RetryPolicy, nowMS int64) Session {
	return Session{Request: req, Policy: policy, Started: nowMS}
}

func (s Session) IsCorrelated(resp contracts.ReasoningResponse) bool {
	return s.Request.RequestID == resp.RequestID
}

func (s Session) IsTimedOut(nowMS int64) bool {
	return s.Policy.TimeoutMS > 0 && nowMS-s.Started >= s.Policy.TimeoutMS
}

func (s Session) AttemptsRemaining() int {
	remaining := s.Policy.TotalAttempts() - s.Attempts
	if remaining < 0 {
		return 0
	}
	return remaining
}

func (s *Session) NextAttempt(nowMS int64) (contracts.ReasoningRequest, bool) {
	if s.AttemptsRemaining() == 0 {
		return contracts.ReasoningRequest{}, false
	}
	s.Attempts++
	s.Started = nowMS
	return s.Request, true
}

type Registry struct {
	mu       sync.Mutex
	sessions map[string]Session
}

func NewRegistry() *Registry {
	return &Registry{sessions: map[string]Session{}}
}

func (r *Registry) Register(session Session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessions[session.Request.RequestID] = session
}

func (r *Registry) Correlate(resp contracts.ReasoningResponse) (Session, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sess, ok := r.sessions[resp.RequestID]
	if ok {
		delete(r.sessions, resp.RequestID)
	}
	return sess, ok
}

func (r *Registry) EvictTimedOut(nowMS int64) []Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	evicted := []Session{}
	for requestID, sess := range r.sessions {
		if sess.IsTimedOut(nowMS) {
			evicted = append(evicted, sess)
			delete(r.sessions, requestID)
		}
	}
	return evicted
}
