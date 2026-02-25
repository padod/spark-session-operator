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

package proxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	sparkv1alpha1 "github.com/tander/spark-session-operator/api/v1alpha1"
	"github.com/tander/spark-session-operator/internal/auth"
)

const (
	sessionPollInterval = 500 * time.Millisecond
	sessionPollTimeout  = 60 * time.Second
	keepaliveInterval   = 2 * time.Minute
	backendDialTimeout  = 10 * time.Second
)

// SessionProxy handles incoming Thrift and gRPC connections, auto-creating sessions
// and proxying traffic to the assigned backend.
type SessionProxy struct {
	client    client.Client
	log       logr.Logger
	namespace string
	auth      *auth.Authenticator
}

// NewSessionProxy creates a new proxy.
func NewSessionProxy(c client.Client, log logr.Logger, namespace string, authenticator *auth.Authenticator) *SessionProxy {
	return &SessionProxy{
		client:    c,
		log:       log.WithName("proxy"),
		namespace: namespace,
		auth:      authenticator,
	}
}

// findPool finds exactly one pool of the given type in the namespace.
// Returns the pool name or an error if 0 or 2+ pools match.
func (p *SessionProxy) findPool(ctx context.Context, poolType string) (string, error) {
	poolList := &sparkv1alpha1.SparkSessionPoolList{}
	if err := p.client.List(ctx, poolList, client.InNamespace(p.namespace)); err != nil {
		return "", fmt.Errorf("list pools: %w", err)
	}

	var matches []string
	for _, pool := range poolList.Items {
		if pool.Spec.Type == poolType {
			matches = append(matches, pool.Name)
		}
	}

	switch len(matches) {
	case 0:
		return "", fmt.Errorf("no pool of type %q found in namespace %s", poolType, p.namespace)
	case 1:
		return matches[0], nil
	default:
		return "", fmt.Errorf("found %d pools of type %q in namespace %s (expected exactly 1): %v",
			len(matches), poolType, p.namespace, matches)
	}
}

// createSession creates a SparkInteractiveSession CR for the user.
func (p *SessionProxy) createSession(ctx context.Context, username, poolName string) (string, error) {
	// Sanitize username for RFC 1123 subdomain: lowercase, replace underscores/dots with hyphens.
	safeName := strings.ToLower(username)
	safeName = strings.NewReplacer("_", "-", ".", "-").Replace(safeName)
	sessionName := fmt.Sprintf("session-%s-%d", safeName, time.Now().UnixNano()%100000)
	session := &sparkv1alpha1.SparkInteractiveSession{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sessionName,
			Namespace: p.namespace,
			Labels: map[string]string{
				"sparkinteractive.io/user":   username,
				"sparkinteractive.io/pool":   poolName,
				"sparkinteractive.io/origin": "proxy",
			},
		},
		Spec: sparkv1alpha1.SparkInteractiveSessionSpec{
			User: username,
			Pool: poolName,
		},
	}

	if err := p.client.Create(ctx, session); err != nil {
		return "", fmt.Errorf("create session: %w", err)
	}

	p.log.Info("Session created", "name", sessionName, "user", username, "pool", poolName)
	return sessionName, nil
}

// waitForSessionActive polls the session CR until it reaches Active state.
// Returns the endpoint or an error on timeout/failure.
func (p *SessionProxy) waitForSessionActive(ctx context.Context, sessionName string) (string, error) {
	deadline := time.Now().Add(sessionPollTimeout)
	ticker := time.NewTicker(sessionPollInterval)
	defer ticker.Stop()

	for {
		session := &sparkv1alpha1.SparkInteractiveSession{}
		if err := p.client.Get(ctx, client.ObjectKey{
			Namespace: p.namespace,
			Name:      sessionName,
		}, session); err != nil {
			return "", fmt.Errorf("get session %s: %w", sessionName, err)
		}

		switch session.Status.State {
		case "Active", "Idle":
			if session.Status.Endpoint == "" {
				return "", fmt.Errorf("session %s is %s but has no endpoint", sessionName, session.Status.State)
			}
			return session.Status.Endpoint, nil
		case "Failed", "Terminated", "Terminating":
			return "", fmt.Errorf("session %s entered state %s", sessionName, session.Status.State)
		}

		if time.Now().After(deadline) {
			return "", fmt.Errorf("timeout waiting for session %s to become active (current state: %s)", sessionName, session.Status.State)
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
}

// updateLastActivity updates the session's LastActivityAt timestamp and transitions Idle→Active.
func (p *SessionProxy) updateLastActivity(ctx context.Context, sessionName string) error {
	session := &sparkv1alpha1.SparkInteractiveSession{}
	if err := p.client.Get(ctx, client.ObjectKey{
		Namespace: p.namespace,
		Name:      sessionName,
	}, session); err != nil {
		return fmt.Errorf("get session for keepalive: %w", err)
	}

	now := metav1.Now()
	session.Status.LastActivityAt = &now
	if session.Status.State == "Idle" {
		session.Status.State = "Active"
	}

	if err := p.client.Status().Update(ctx, session); err != nil {
		return fmt.Errorf("update session activity: %w", err)
	}
	return nil
}

// StartThriftProxy starts the TCP listener for Thrift connections.
func (p *SessionProxy) StartThriftProxy(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	p.log.Info("Starting Thrift proxy", "addr", addr)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				p.log.Error(err, "Failed to accept Thrift connection")
				continue
			}
			go p.handleThriftConnection(conn)
		}
	}()

	return nil
}

// handleThriftConnection handles a single Thrift client connection.
func (p *SessionProxy) handleThriftConnection(clientConn net.Conn) {
	defer clientConn.Close()

	remoteAddr := clientConn.RemoteAddr().String()
	p.log.V(1).Info("New Thrift connection", "remote", remoteAddr)

	ctx := context.Background()

	// 1. Extract SASL credentials
	saslAuth, rawBytes, err := ExtractThriftSASLAuth(clientConn)
	if err != nil {
		p.log.Error(err, "SASL auth extraction failed", "remote", remoteAddr)
		_ = writeSASLFrame(clientConn, saslERROR, []byte(err.Error()))
		return
	}

	// 2. Authenticate via Keycloak ROPC
	userInfo, err := p.auth.AuthenticateWithCredentials(ctx, saslAuth.Username, saslAuth.Password)
	if err != nil {
		p.log.Error(err, "Credential authentication failed", "user", saslAuth.Username, "remote", remoteAddr)
		_ = writeSASLFrame(clientConn, saslERROR, []byte("authentication failed"))
		return
	}

	p.log.Info("Thrift user authenticated", "user", userInfo.Username, "remote", remoteAddr)

	// 3. Find pool
	poolName, err := p.findPool(ctx, "thrift")
	if err != nil {
		p.log.Error(err, "Failed to find thrift pool", "user", userInfo.Username)
		_ = writeSASLFrame(clientConn, saslERROR, []byte(err.Error()))
		return
	}

	// 4. Create session
	sessionName, err := p.createSession(ctx, userInfo.Username, poolName)
	if err != nil {
		p.log.Error(err, "Failed to create session", "user", userInfo.Username, "pool", poolName)
		_ = writeSASLFrame(clientConn, saslERROR, []byte("failed to create session"))
		return
	}

	// 5. Wait for session to become active
	endpoint, err := p.waitForSessionActive(ctx, sessionName)
	if err != nil {
		p.log.Error(err, "Session did not become active", "session", sessionName)
		_ = writeSASLFrame(clientConn, saslERROR, []byte("session failed to start"))
		return
	}

	// 6. Connect to backend
	backendConn, err := net.DialTimeout("tcp", endpoint, backendDialTimeout)
	if err != nil {
		p.log.Error(err, "Failed to connect to backend", "endpoint", endpoint, "session", sessionName)
		_ = writeSASLFrame(clientConn, saslERROR, []byte("backend connection failed"))
		return
	}
	defer backendConn.Close()

	// 7. Replay raw SASL bytes to backend
	if _, err := backendConn.Write(rawBytes); err != nil {
		p.log.Error(err, "Failed to replay SASL to backend", "session", sessionName)
		_ = writeSASLFrame(clientConn, saslERROR, []byte("backend handshake failed"))
		return
	}

	// Read backend SASL COMPLETE
	backendStatus, _, _, err := readSASLFrame(backendConn)
	if err != nil {
		p.log.Error(err, "Failed to read backend SASL response", "session", sessionName)
		_ = writeSASLFrame(clientConn, saslERROR, []byte("backend handshake failed"))
		return
	}
	if backendStatus != saslComplete {
		p.log.Error(nil, "Backend SASL handshake failed", "status", backendStatus, "session", sessionName)
		_ = writeSASLFrame(clientConn, saslERROR, []byte("backend authentication failed"))
		return
	}

	// 8. Forward SASL COMPLETE to client
	if err := CompleteSASLHandshake(clientConn); err != nil {
		p.log.Error(err, "Failed to send SASL COMPLETE to client", "session", sessionName)
		return
	}

	p.log.Info("Thrift session proxying started", "session", sessionName, "user", userInfo.Username, "endpoint", endpoint)

	// 9. Bidirectional proxy with keepalive
	p.proxyWithKeepalive(clientConn, backendConn, sessionName)
}

// StartConnectProxy starts the gRPC server for Spark Connect connections.
func (p *SessionProxy) StartConnectProxy(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	server := grpc.NewServer(
		grpc.ForceServerCodec(rawCodec{}),
		grpc.UnknownServiceHandler(p.handleConnectStream),
	)

	p.log.Info("Starting Connect gRPC proxy", "addr", addr)

	go func() {
		if err := server.Serve(listener); err != nil {
			p.log.Error(err, "gRPC server failed")
		}
	}()

	return nil
}

// handleConnectStream handles a single gRPC stream for Spark Connect.
func (p *SessionProxy) handleConnectStream(_ interface{}, serverStream grpc.ServerStream) error {
	ctx := serverStream.Context()

	var username, password string
	var firstMsg *rawFrame

	// 1. Try gRPC metadata auth first (works on TLS channels)
	username, password, metaErr := extractCredentialsFromGRPCMetadata(ctx)
	if metaErr != nil {
		// 2. Fall back: read the first protobuf message and extract user_context.user_id.
		//    On insecure channels PySpark cannot send gRPC metadata credentials,
		//    but user_id is always embedded in the protobuf request body.
		//    Users set it via: sc://host:port/;user_id=base64(user:pass)
		//    or:               sc://host:port/;user_id=username
		firstMsg = &rawFrame{}
		if err := serverStream.RecvMsg(firstMsg); err != nil {
			p.log.Error(err, "Failed to read first gRPC message")
			return status.Errorf(codes.Internal, "failed to read request: %v", err)
		}

		var protoErr error
		username, password, protoErr = extractCredentialsFromProto(firstMsg.payload)
		if protoErr != nil {
			p.log.Error(protoErr, "Failed to extract credentials from protobuf")
			return status.Errorf(codes.Unauthenticated,
				"missing credentials: set user_id in sc:// URL, e.g. sc://host:port/;user_id=base64(user:pass)")
		}
	}

	// 3. Authenticate via Keycloak ROPC (or skip-validation)
	userInfo, err := p.auth.AuthenticateWithCredentials(ctx, username, password)
	if err != nil {
		p.log.Error(err, "Credential authentication failed", "user", username)
		return status.Errorf(codes.Unauthenticated, "authentication failed")
	}

	p.log.Info("Connect user authenticated", "user", userInfo.Username)

	// 4. Find pool
	poolName, err := p.findPool(ctx, "connect")
	if err != nil {
		p.log.Error(err, "Failed to find connect pool", "user", userInfo.Username)
		return status.Errorf(codes.FailedPrecondition, "%v", err)
	}

	// 5. Create session
	sessionName, err := p.createSession(ctx, userInfo.Username, poolName)
	if err != nil {
		p.log.Error(err, "Failed to create session", "user", userInfo.Username, "pool", poolName)
		return status.Errorf(codes.Internal, "failed to create session")
	}

	// 6. Wait for session to become active
	endpoint, err := p.waitForSessionActive(ctx, sessionName)
	if err != nil {
		p.log.Error(err, "Session did not become active", "session", sessionName)
		return status.Errorf(codes.Unavailable, "session failed to start")
	}

	// 7. Connect to backend gRPC server
	backendConn, err := grpc.NewClient(endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype("raw")),
	)
	if err != nil {
		p.log.Error(err, "Failed to connect to backend", "endpoint", endpoint, "session", sessionName)
		return status.Errorf(codes.Unavailable, "backend connection failed")
	}
	defer backendConn.Close()

	// Get the full method name from the transport stream
	fullMethod, ok := grpc.MethodFromServerStream(serverStream)
	if !ok {
		return status.Errorf(codes.Internal, "failed to get method name")
	}

	// 8. Create backend client stream
	backendCtx, backendCancel := context.WithCancel(ctx)
	defer backendCancel()

	backendStream, err := backendConn.NewStream(backendCtx, &grpc.StreamDesc{
		ServerStreams: true,
		ClientStreams: true,
	}, fullMethod, grpc.CallContentSubtype("raw"))
	if err != nil {
		p.log.Error(err, "Failed to create backend stream", "session", sessionName, "method", fullMethod)
		return status.Errorf(codes.Unavailable, "backend stream failed")
	}

	p.log.Info("Connect session proxying started", "session", sessionName, "user", userInfo.Username, "endpoint", endpoint, "method", fullMethod)

	// 9. If we consumed the first message for auth, forward it to the backend now
	if firstMsg != nil {
		if err := backendStream.SendMsg(firstMsg); err != nil {
			p.log.Error(err, "Failed to forward first message to backend", "session", sessionName)
			return status.Errorf(codes.Unavailable, "failed to forward request to backend")
		}
	}

	// Start keepalive
	keepaliveCtx, keepaliveCancel := context.WithCancel(context.Background())
	defer keepaliveCancel()
	go p.runKeepalive(keepaliveCtx, sessionName)

	// Bidirectional gRPC stream proxy
	errCh := make(chan error, 2)

	// Server→Backend (client sends to proxy, proxy forwards to backend)
	go func() {
		for {
			frame := &rawFrame{}
			if err := serverStream.RecvMsg(frame); err != nil {
				if err == io.EOF {
					_ = backendStream.CloseSend()
					errCh <- nil
					return
				}
				errCh <- err
				return
			}
			if err := backendStream.SendMsg(frame); err != nil {
				errCh <- err
				return
			}
		}
	}()

	// Backend→Server (backend responds, proxy forwards to client)
	go func() {
		for {
			frame := &rawFrame{}
			if err := backendStream.RecvMsg(frame); err != nil {
				if err == io.EOF {
					errCh <- nil
					return
				}
				errCh <- err
				return
			}
			if err := serverStream.SendMsg(frame); err != nil {
				errCh <- err
				return
			}
		}
	}()

	// Wait for either direction to finish
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			p.log.V(1).Info("gRPC stream ended", "session", sessionName, "error", err)
			return err
		}
	}

	return nil
}

// proxyWithKeepalive runs bidirectional TCP copy with periodic keepalive updates.
func (p *SessionProxy) proxyWithKeepalive(clientConn, backendConn net.Conn, sessionName string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start keepalive goroutine
	go p.runKeepalive(ctx, sessionName)

	// Bidirectional copy
	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(backendConn, clientConn)
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(clientConn, backendConn)
		done <- struct{}{}
	}()

	// Wait for either direction to close
	<-done
	p.log.Info("Thrift session proxy closed", "session", sessionName)
}

// runKeepalive periodically updates LastActivityAt for the session.
func (p *SessionProxy) runKeepalive(ctx context.Context, sessionName string) {
	ticker := time.NewTicker(keepaliveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.updateLastActivity(context.Background(), sessionName); err != nil {
				p.log.Error(err, "Keepalive update failed", "session", sessionName)
			}
		}
	}
}
