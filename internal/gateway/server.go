/*
Copyright 2026 Tander.

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
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/gorilla/mux"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sparkv1alpha1 "github.com/tander/spark-session-operator/api/v1alpha1"
	"github.com/tander/spark-session-operator/internal/auth"
)

// SessionGateway provides REST API for session management
type SessionGateway struct {
	client    client.Client
	log       logr.Logger
	namespace string
	auth      *auth.Authenticator
}

// NewSessionGateway creates a new gateway
func NewSessionGateway(c client.Client, log logr.Logger, namespace string, authenticator *auth.Authenticator) *SessionGateway {
	return &SessionGateway{
		client:    c,
		log:       log.WithName("gateway"),
		namespace: namespace,
		auth:      authenticator,
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

// Start starts the HTTP server
func (g *SessionGateway) Start(addr string) error {
	router := mux.NewRouter()

	api := router.PathPrefix("/api/v1").Subrouter()
	api.Use(g.authMiddleware)

	api.HandleFunc("/pools", g.listPools).Methods("GET")
	api.HandleFunc("/sessions", g.listSessions).Methods("GET")
	api.HandleFunc("/sessions/{name}", g.getSession).Methods("GET")
	api.HandleFunc("/sessions/{name}", g.deleteSession).Methods("DELETE")

	// Health endpoints (no auth)
	router.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}).Methods("GET")

	router.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}).Methods("GET")

	g.log.Info("Starting session gateway", "addr", addr)
	return http.ListenAndServe(addr, router)
}

// authMiddleware extracts and validates the OIDC token
func (g *SessionGateway) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userInfo, err := g.extractUser(r)
		if err != nil {
			g.writeError(w, http.StatusUnauthorized, "unauthorized", err.Error())
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
		g.writeError(w, http.StatusInternalServerError, "list_failed", err.Error())
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

	g.writeJSON(w, http.StatusOK, responses)
}

func (g *SessionGateway) listSessions(w http.ResponseWriter, r *http.Request) {
	userInfo := r.Context().Value(userInfoKey).(*auth.UserInfo)

	sessionList := &sparkv1alpha1.SparkInteractiveSessionList{}
	if err := g.client.List(r.Context(), sessionList,
		client.InNamespace(g.namespace),
		client.MatchingLabels{"sparkinteractive.io/user": userInfo.Username},
	); err != nil {
		g.writeError(w, http.StatusInternalServerError, "list_failed", err.Error())
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
		g.writeError(w, http.StatusInternalServerError, "terminate_failed", err.Error())
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

func (g *SessionGateway) writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func (g *SessionGateway) writeError(w http.ResponseWriter, status int, errCode, message string) {
	g.writeJSON(w, status, ErrorResponse{Error: errCode, Message: message})
}
