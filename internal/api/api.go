// Package api is Strata's HTTP/JSON control-plane API.
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/udaykishore-resu/strata/internal/auth"
	"github.com/udaykishore-resu/strata/internal/engine"
	"github.com/udaykishore-resu/strata/internal/obs"
	"github.com/udaykishore-resu/strata/internal/state"
	"github.com/udaykishore-resu/strata/pkg/template"
)

// Server serves the API.
type Server struct {
	Engine       *engine.Engine
	Auth         auth.Authenticator
	Log          *slog.Logger
	Metrics      *obs.Metrics
	MaxBodyBytes int64
	Version      string
	// HealthOnly serves only health and metrics endpoints (worker-only processes).
	HealthOnly bool
}

// Handler returns the root HTTP handler with middleware applied.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /readyz", s.ready)
	if s.Metrics != nil {
		mux.Handle("GET /metrics", s.Metrics.Registry.Handler())
	}
	if s.HealthOnly {
		return s.instrument(s.recoverer(mux))
	}
	authed := func(pattern string, h http.HandlerFunc) { mux.Handle(pattern, s.authenticate(h)) }
	authed("GET /v1/version", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]string{"version": s.Version, "project": s.Engine.Project, "region": s.Engine.Region})
	})
	authed("GET /v1/resource-types", s.resourceTypes)
	authed("GET /v1/stacks", s.listStacks)
	authed("GET /v1/stacks/{stack}", s.getStack)
	authed("DELETE /v1/stacks/{stack}", s.deleteStack)
	authed("POST /v1/stacks/{stack}/changesets", s.createChangeSet)
	authed("GET /v1/stacks/{stack}/changesets/{id}", s.getChangeSet)
	authed("POST /v1/stacks/{stack}/changesets/{id}/execute", s.executeChangeSet)
	authed("GET /v1/stacks/{stack}/events", s.listEvents)
	authed("POST /v1/stacks/{stack}/drift", s.detectDrift)
	authed("GET /v1/operations/{id}", s.getOperation)
	return s.instrument(s.recoverer(mux))
}

// ---------------------------------------------------------------- handlers

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if err := s.Engine.Store.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "store unavailable: "+err.Error())
		return
	}
	writeJSON(w, 200, map[string]string{"status": "ready"})
}

func (s *Server) resourceTypes(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{"resourceTypes": s.Engine.Registry.Schemas()})
}

// StackSummary is the list view of a stack.
type StackSummary struct {
	Name             string            `json:"name"`
	Status           state.StackStatus `json:"status"`
	StatusReason     string            `json:"statusReason,omitempty"`
	Version          int64             `json:"version"`
	Resources        int               `json:"resources"`
	CurrentOperation string            `json:"currentOperation,omitempty"`
	UpdatedAt        time.Time         `json:"updatedAt"`
}

func (s *Server) listStacks(w http.ResponseWriter, r *http.Request) {
	stacks, err := s.Engine.Store.ListStacks(r.Context())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	out := make([]StackSummary, 0, len(stacks))
	for _, st := range stacks {
		out = append(out, StackSummary{
			Name: st.Name, Status: st.Status, StatusReason: st.StatusReason, Version: st.Version,
			Resources: len(st.Resources), CurrentOperation: st.CurrentOperation, UpdatedAt: st.UpdatedAt,
		})
	}
	writeJSON(w, 200, map[string]any{"stacks": out})
}

func (s *Server) getStack(w http.ResponseWriter, r *http.Request) {
	st, err := s.Engine.Store.GetStack(r.Context(), r.PathValue("stack"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, 200, st)
}

func (s *Server) deleteStack(w http.ResponseWriter, r *http.Request) {
	op, err := s.Engine.StartDelete(r.Context(), r.PathValue("stack"), caller(r))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.audit(r, "stack.delete", "stack", op.Stack, "operation", op.ID)
	writeJSON(w, http.StatusAccepted, op)
}

// ChangeSetRequest is the body of POST /v1/stacks/{stack}/changesets.
type ChangeSetRequest struct {
	Template   json.RawMessage `json:"template"`
	Parameters map[string]any  `json:"parameters,omitempty"`
	// Refresh reads live resources before planning so out-of-band changes
	// are detected and restored. Defaults to true.
	Refresh *bool `json:"refresh,omitempty"`
}

func (s *Server) createChangeSet(w http.ResponseWriter, r *http.Request) {
	var req ChangeSetRequest
	if err := s.decode(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(req.Template) == 0 {
		writeError(w, http.StatusBadRequest, "template is required")
		return
	}
	tmpl, err := template.Parse(req.Template)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	opts := engine.PlanOptions{Refresh: req.Refresh == nil || *req.Refresh}
	cs, err := s.Engine.PlanWith(r.Context(), r.PathValue("stack"), tmpl, req.Parameters, caller(r), opts)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.audit(r, "changeset.create", "stack", cs.Stack, "changeSet", cs.ID, "hasChanges", cs.HasChanges())
	writeJSON(w, http.StatusCreated, cs)
}

func (s *Server) getChangeSet(w http.ResponseWriter, r *http.Request) {
	cs, err := s.Engine.Store.GetChangeSet(r.Context(), r.PathValue("id"))
	if err == nil && cs.Stack != r.PathValue("stack") {
		err = state.ErrNotFound
	}
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, 200, cs)
}

func (s *Server) executeChangeSet(w http.ResponseWriter, r *http.Request) {
	op, err := s.Engine.StartDeploy(r.Context(), r.PathValue("stack"), r.PathValue("id"), caller(r))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.audit(r, "changeset.execute", "stack", op.Stack, "changeSet", op.ChangeSetID, "operation", op.ID)
	writeJSON(w, http.StatusAccepted, op)
}

func (s *Server) listEvents(w http.ResponseWriter, r *http.Request) {
	after, _ := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	evs, err := s.Engine.Store.ListEvents(r.Context(), r.PathValue("stack"), after, limit)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if evs == nil {
		evs = []state.Event{}
	}
	writeJSON(w, 200, map[string]any{"events": evs})
}

func (s *Server) detectDrift(w http.ResponseWriter, r *http.Request) {
	rep, err := s.Engine.DetectDrift(r.Context(), r.PathValue("stack"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, 200, rep)
}

func (s *Server) getOperation(w http.ResponseWriter, r *http.Request) {
	op, err := s.Engine.Store.GetOperation(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	// The checkpoint is internal resume state; expose a compact view.
	op.Checkpoint = state.Checkpoint{Phase: op.Checkpoint.Phase, Failures: op.Checkpoint.Failures, RollbackErrors: op.Checkpoint.RollbackErrors}
	writeJSON(w, 200, op)
}

// -------------------------------------------------------------- middleware

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

func (s *Server) instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		reqID := r.Header.Get("X-Request-Id")
		if reqID == "" {
			b := make([]byte, 8)
			_, _ = rand.Read(b)
			reqID = hex.EncodeToString(b)
		}
		w.Header().Set("X-Request-Id", reqID)
		sw := &statusWriter{ResponseWriter: w, code: 200}
		next.ServeHTTP(sw, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		dur := time.Since(start)
		if s.Metrics != nil {
			s.Metrics.HTTPRequests.Inc(r.Method, route, strconv.Itoa(sw.code))
			s.Metrics.HTTPDuration.Observe(dur.Seconds(), r.Method, route)
		}
		if strings.HasPrefix(r.URL.Path, "/v1/") {
			attrs := []any{"method", r.Method, "path", r.URL.Path, "status", sw.code, "durationMs", dur.Milliseconds(), "requestId", reqID}
			if tr := r.Header.Get("X-Cloud-Trace-Context"); tr != "" && s.Engine.Project != "" {
				attrs = append(attrs, "logging.googleapis.com/trace", fmt.Sprintf("projects/%s/traces/%s", s.Engine.Project, strings.Split(tr, "/")[0]))
			}
			s.logger().Info("http request", attrs...)
		}
	})
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				s.logger().Error("panic serving request", "panic", fmt.Sprint(v), "path", r.URL.Path)
				writeError(w, http.StatusInternalServerError, "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, err := s.Auth.Authenticate(r)
		switch {
		case errors.Is(err, auth.ErrForbidden):
			writeError(w, http.StatusForbidden, err.Error())
			return
		case err != nil:
			w.Header().Set("WWW-Authenticate", `Bearer realm="strata"`)
			writeError(w, http.StatusUnauthorized, err.Error())
			return
		}
		next.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), p)))
	})
}

// ------------------------------------------------------------------ helpers

func (s *Server) logger() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}

func (s *Server) audit(r *http.Request, action string, kv ...any) {
	s.logger().Info("audit", append([]any{"action", action, "caller", caller(r)}, kv...)...)
}

func caller(r *http.Request) string {
	if p := auth.FromContext(r.Context()); p != nil {
		return p.Email
	}
	return ""
}

func (s *Server) decode(w http.ResponseWriter, r *http.Request, v any) error {
	limit := s.MaxBodyBytes
	if limit <= 0 {
		limit = 5 << 20
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, limit))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("invalid request body: %w", err)
	}
	return nil
}

func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	var ve *engine.ValidationError
	switch {
	case errors.As(err, &ve):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, state.ErrNotFound):
		writeError(w, http.StatusNotFound, "not found")
	case errors.Is(err, state.ErrConflict):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, context.Canceled):
		writeError(w, 499, "request cancelled")
	default:
		s.logger().Error("request failed", "path", r.URL.Path, "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

// ErrorBody is the JSON error envelope.
type ErrorBody struct {
	Error struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func writeError(w http.ResponseWriter, code int, msg string) {
	var b ErrorBody
	b.Error.Code, b.Error.Message = code, msg
	writeJSON(w, code, b)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
