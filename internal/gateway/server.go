/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package gateway

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/gorilla/mux"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sparkv1alpha1 "github.com/padod/spark-session-operator/api/v1alpha1"
	"github.com/padod/spark-session-operator/internal/auth"
)

//go:embed templates/pools.html
var poolsTmplFS embed.FS

// maxAPIBodyBytes caps inbound request bodies on /api/v1/* endpoints. The
// gateway only ever consumes small JSON payloads, so 1 MiB is generous and
// keeps memory bounded against trivial DoS via oversized POST bodies.
const maxAPIBodyBytes = 1 << 20

// gatewayShutdownTimeout bounds the graceful drain of in-flight HTTP
// requests on SIGTERM. Sized to fit comfortably under a typical
// terminationGracePeriodSeconds of 30s.
const gatewayShutdownTimeout = 20 * time.Second

// SessionGateway provides REST API for session management
type SessionGateway struct {
	client    client.Client
	log       logr.Logger
	namespace string
	auth      *auth.Authenticator
	limiter   *ipRateLimiter

	// srv is set on Start so callers can invoke Shutdown for a graceful
	// drain on SIGTERM. nil until Start is called.
	srv *http.Server
}

// NewSessionGateway creates a new gateway
func NewSessionGateway(c client.Client, log logr.Logger, namespace string, authenticator *auth.Authenticator) *SessionGateway {
	return &SessionGateway{
		client:    c,
		log:       log.WithName("gateway"),
		namespace: namespace,
		auth:      authenticator,
		limiter:   newIPRateLimiter(defaultRateLimit, defaultRateBurst, defaultIdleTTL),
	}
}

// SessionResponse returned to the user
type SessionResponse struct {
	Name             string            `json:"name"`
	User             string            `json:"user"`
	Pool             string            `json:"pool"`
	State            string            `json:"state"`
	AssignedInstance string            `json:"assignedInstance,omitempty"`
	SparkConf        map[string]string `json:"sparkConf,omitempty"`
	CreatedAt        *time.Time        `json:"createdAt,omitempty"`
	LastActivityAt   *time.Time        `json:"lastActivityAt,omitempty"`
}

// ErrorResponse for API errors
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// Start starts the HTTP server. Returns http.ErrServerClosed when Shutdown is
// called (graceful path). Any other non-nil error is a hard failure.
func (g *SessionGateway) Start(addr string) error {
	router := mux.NewRouter()

	api := router.PathPrefix("/api/v1").Subrouter()
	// Order matters: cap body size first, then rate-limit, then authenticate.
	// MaxBytesReader is cheap; rate-limiting before auth ensures unauthenticated
	// brute-force traffic still pays the per-IP budget.
	api.Use(g.bodyLimitMiddleware(maxAPIBodyBytes))
	api.Use(g.rateLimitMiddleware)
	api.Use(g.authMiddleware)

	api.HandleFunc("/sessions", g.listSessions).Methods("GET")
	api.HandleFunc("/sessions/{name}", g.getSession).Methods("GET")
	api.HandleFunc("/sessions/{name}", g.deleteSession).Methods("DELETE")

	// Public endpoints (no auth, no rate limit, no body cap). /api/v1/pools
	// is intentionally unauthenticated so the HTML dashboard is openable in a
	// browser without a token; the stored-XSS vector that previously existed
	// on user-controlled pool fields is closed in writePoolsHTML via
	// html/template auto-escaping.
	router.HandleFunc("/api/v1/pools", g.listPools).Methods("GET")
	router.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}).Methods("GET")

	router.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}).Methods("GET")

	g.srv = &http.Server{
		Addr:              addr,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}
	g.log.Info("Starting session gateway", "addr", addr)
	return g.srv.ListenAndServe()
}

// Shutdown gracefully drains in-flight requests. Safe to call before Start
// (no-op). Returns whatever http.Server.Shutdown returns.
func (g *SessionGateway) Shutdown(ctx context.Context) error {
	if g.srv == nil {
		return nil
	}
	return g.srv.Shutdown(ctx)
}

// Run starts the gateway and blocks until ctx is canceled, then performs a
// graceful shutdown bounded by gatewayShutdownTimeout. Designed for
// controller-runtime's manager.Add so SIGTERM drains the API instead of
// dropping in-flight requests.
func (g *SessionGateway) Run(ctx context.Context, addr string) error {
	serveErr := make(chan error, 1)
	go func() {
		if err := g.Start(addr); err != nil && err != http.ErrServerClosed {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), gatewayShutdownTimeout)
		defer cancel()
		g.log.Info("Shutting down session gateway")
		return g.Shutdown(shutdownCtx)
	case err := <-serveErr:
		return err
	}
}

// bodyLimitMiddleware wraps the request body with http.MaxBytesReader so
// oversized POST/PUT/DELETE payloads are rejected before handlers allocate.
func (g *SessionGateway) bodyLimitMiddleware(limit int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Body = http.MaxBytesReader(w, r.Body, limit)
			next.ServeHTTP(w, r)
		})
	}
}

// rateLimitMiddleware enforces a per-source-IP token bucket on the protected
// API surface. The limiter keys on the immediate TCP peer; X-Forwarded-For is
// not consulted (see clientIP).
func (g *SessionGateway) rateLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := clientIP(r)
		if !g.limiter.allow(ip) {
			g.writeError(w, http.StatusTooManyRequests, "rate_limited", "Too many requests")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// authMiddleware extracts and validates the OIDC token
func (g *SessionGateway) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userInfo, err := g.extractUser(r)
		if err != nil {
			// Auth errors may carry JWKS/issuer/parsing detail useful to an
			// attacker probing the IDP. Log server-side, return generic 401.
			g.log.V(1).Info("Authentication failed", "remote", r.RemoteAddr, "err", err.Error())
			g.writeError(w, http.StatusUnauthorized, "unauthorized", "Unauthorized")
			return
		}

		// Store user info in context
		ctx := context.WithValue(r.Context(), userInfoKey, userInfo)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// contextKey is an unexported type for context keys to avoid collisions.
type contextKey string

const userInfoKey contextKey = "userInfo"

func (g *SessionGateway) extractUser(r *http.Request) (*auth.UserInfo, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return nil, fmt.Errorf("missing Authorization header")
	}

	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
		return nil, fmt.Errorf("invalid Authorization header format")
	}

	return g.auth.ValidateToken(parts[1])
}

// PoolResponse returned for pool listing
type PoolResponse struct {
	Name                string            `json:"name"`
	Type                string            `json:"type"`
	Host                string            `json:"host"`
	MinReplicas         int32             `json:"minReplicas"`
	MaxReplicas         int32             `json:"maxReplicas"`
	CurrentReplicas     int32             `json:"currentReplicas"`
	ReadyReplicas       int32             `json:"readyReplicas"`
	TotalActiveSessions int32             `json:"totalActiveSessions"`
	SessionPolicy       SessionPolicyInfo `json:"sessionPolicy"`
}

// SessionPolicyInfo is a subset of session policy for the API response
type SessionPolicyInfo struct {
	MaxSessionsPerUser int32             `json:"maxSessionsPerUser"`
	MaxTotalSessions   int32             `json:"maxTotalSessions"`
	IdleTimeoutMinutes int32             `json:"idleTimeoutMinutes"`
	DefaultSessionConf map[string]string `json:"defaultSessionConf,omitempty"`
}

func (g *SessionGateway) listPools(w http.ResponseWriter, r *http.Request) {
	poolList := &sparkv1alpha1.SparkSessionPoolList{}
	if err := g.client.List(r.Context(), poolList, client.InNamespace(g.namespace)); err != nil {
		g.serverError(w, "list_failed", "Failed to list pools", err)
		return
	}

	var responses []PoolResponse
	for i := range poolList.Items {
		p := &poolList.Items[i]
		responses = append(responses, PoolResponse{
			Name:                p.Name,
			Type:                p.Spec.Type,
			Host:                p.Spec.Host,
			MinReplicas:         p.Spec.Replicas.Min,
			MaxReplicas:         p.Spec.Replicas.Max,
			CurrentReplicas:     p.Status.CurrentReplicas,
			ReadyReplicas:       p.Status.ReadyReplicas,
			TotalActiveSessions: p.Status.TotalActiveSessions,
			SessionPolicy: SessionPolicyInfo{
				MaxSessionsPerUser: p.Spec.SessionPolicy.MaxSessionsPerUser,
				MaxTotalSessions:   p.Spec.SessionPolicy.MaxTotalSessions,
				IdleTimeoutMinutes: p.Spec.SessionPolicy.IdleTimeoutMinutes,
				DefaultSessionConf: p.Spec.SessionPolicy.DefaultSessionConf,
			},
		})
	}

	// Return HTML table for browsers, JSON for programmatic access.
	// ?format=json forces JSON even from a browser.
	if r.URL.Query().Get("format") != "json" && strings.Contains(r.Header.Get("Accept"), "text/html") {
		g.writePoolsHTML(w, responses)
		return
	}
	g.writeJSON(w, http.StatusOK, responses)
}

// poolsTmpl renders the pools dashboard from templates/pools.html.
// html/template auto-escapes every interpolated value, which closes the
// stored-XSS hole that an fmt.Fprintf-based builder would leave open on
// user-controlled spec fields (Pool.Host has no character restrictions in
// the CRD).
var poolsTmpl = template.Must(template.New("pools.html").Funcs(template.FuncMap{
	"typeClass": func(t string) string {
		if t == "thrift" {
			return "tag-thrift"
		}
		return "tag-connect"
	},
	"readyClass": func(ready, current int32) string {
		switch {
		case ready == 0:
			return "zero"
		case ready < current:
			return "not-ready"
		default:
			return "ready"
		}
	},
	"sessClass": func(n int32) string {
		if n == 0 {
			return "zero"
		}
		return ""
	},
}).ParseFS(poolsTmplFS, "templates/pools.html"))

func (g *SessionGateway) writePoolsHTML(w http.ResponseWriter, pools []PoolResponse) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := poolsTmpl.Execute(w, pools); err != nil {
		// Header may already be flushed; just log so the operator notices.
		g.log.Error(err, "render pools HTML")
	}
}

func (g *SessionGateway) listSessions(w http.ResponseWriter, r *http.Request) {
	userInfo := r.Context().Value(userInfoKey).(*auth.UserInfo)

	sessionList := &sparkv1alpha1.SparkInteractiveSessionList{}
	if err := g.client.List(r.Context(), sessionList,
		client.InNamespace(g.namespace),
		client.MatchingLabels{"sparkinteractive.io/user": userInfo.Username},
	); err != nil {
		g.serverError(w, "list_failed", "Failed to list sessions", err)
		return
	}

	var responses []SessionResponse
	for i := range sessionList.Items {
		responses = append(responses, g.sessionToResponse(&sessionList.Items[i]))
	}

	g.writeJSON(w, http.StatusOK, responses)
}

func (g *SessionGateway) getSession(w http.ResponseWriter, r *http.Request) {
	userInfo := r.Context().Value(userInfoKey).(*auth.UserInfo)
	name := mux.Vars(r)["name"]

	session := &sparkv1alpha1.SparkInteractiveSession{}
	if err := g.client.Get(r.Context(), types.NamespacedName{
		Namespace: g.namespace,
		Name:      name,
	}, session); err != nil {
		g.writeError(w, http.StatusNotFound, "not_found", "Session not found")
		return
	}

	// Users can only see their own sessions
	if session.Spec.User != userInfo.Username {
		g.writeError(w, http.StatusForbidden, "forbidden", "Not your session")
		return
	}

	g.writeJSON(w, http.StatusOK, g.sessionToResponse(session))
}

func (g *SessionGateway) deleteSession(w http.ResponseWriter, r *http.Request) {
	userInfo := r.Context().Value(userInfoKey).(*auth.UserInfo)
	name := mux.Vars(r)["name"]

	session := &sparkv1alpha1.SparkInteractiveSession{}
	if err := g.client.Get(r.Context(), types.NamespacedName{
		Namespace: g.namespace,
		Name:      name,
	}, session); err != nil {
		g.writeError(w, http.StatusNotFound, "not_found", "Session not found")
		return
	}

	if session.Spec.User != userInfo.Username {
		g.writeError(w, http.StatusForbidden, "forbidden", "Not your session")
		return
	}

	// Set state to Terminating
	session.Status.State = "Terminating"
	if err := g.client.Status().Update(r.Context(), session); err != nil {
		g.serverError(w, "terminate_failed", "Failed to terminate session", err)
		return
	}

	g.log.Info("Session termination requested", "name", name, "user", userInfo.Username)
	g.writeJSON(w, http.StatusOK, g.sessionToResponse(session))
}

func (g *SessionGateway) sessionToResponse(s *sparkv1alpha1.SparkInteractiveSession) SessionResponse {
	resp := SessionResponse{
		Name:             s.Name,
		User:             s.Spec.User,
		Pool:             s.Spec.Pool,
		State:            s.Status.State,
		AssignedInstance: s.Status.AssignedInstance,
		SparkConf:        s.Spec.SparkConf,
	}
	if s.Status.CreatedAt != nil {
		t := s.Status.CreatedAt.Time
		resp.CreatedAt = &t
	}
	if s.Status.LastActivityAt != nil {
		t := s.Status.LastActivityAt.Time
		resp.LastActivityAt = &t
	}
	return resp
}

func (g *SessionGateway) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (g *SessionGateway) writeError(w http.ResponseWriter, status int, errCode, message string) {
	g.writeJSON(w, status, ErrorResponse{Error: errCode, Message: message})
}

// serverError logs the underlying error server-side and returns a generic 500
// response. Internal error text (resource names, namespaces, RBAC details,
// apiserver stack frames) is kept out of the client-facing payload to avoid
// information disclosure.
func (g *SessionGateway) serverError(w http.ResponseWriter, errCode, message string, err error) {
	g.log.Error(err, message, "code", errCode)
	g.writeError(w, http.StatusInternalServerError, errCode, message)
}
