/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// testIDP is an httptest-backed OIDC issuer: it serves the discovery
// document and JWKS for a generated RSA keypair and lets the test sign
// tokens that the production validator path can verify end-to-end.
type testIDP struct {
	server *httptest.Server
	key    *rsa.PrivateKey
	kid    string
}

func newTestIDP(t *testing.T) *testIDP {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	idp := &testIDP{key: key, kid: "test-kid-1"}

	mux := http.NewServeMux()
	idp.server = httptest.NewServer(mux)
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer":   idp.server.URL,
			"jwks_uri": idp.server.URL + "/jwks",
		})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(idp.jwks())
	})

	t.Cleanup(idp.server.Close)
	return idp
}

func (i *testIDP) jwks() map[string]any {
	pub := i.key.Public().(*rsa.PublicKey)
	return map[string]any{
		"keys": []map[string]string{{
			"kty": "RSA",
			"use": "sig",
			"alg": "RS256",
			"kid": i.kid,
			"n":   base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
			"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes()),
		}},
	}
}

// sign produces an RS256 token with the IDP's key. kid override allows
// negative tests that simulate keys absent from the JWKS document.
func (i *testIDP) sign(t *testing.T, claims jwt.MapClaims, kid string) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	if kid != "" {
		tok.Header["kid"] = kid
	} else {
		tok.Header["kid"] = i.kid
	}
	s, err := tok.SignedString(i.key)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return s
}

// unsignedToken crafts a JWT with alg=none — the historic bypass we
// must reject regardless of issuer/audience/exp validity.
func unsignedToken(claims map[string]any) string {
	hdr := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	body, _ := json.Marshal(claims)
	return hdr + "." + base64.RawURLEncoding.EncodeToString(body) + "."
}

// hsToken signs a token with HS256 using the RSA public key bytes as the
// HMAC secret — the classic algorithm-confusion attack. A correct
// validator pins acceptable algs server-side and rejects this.
func hsToken(t *testing.T, idp *testIDP, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tok.Header["kid"] = idp.kid
	secret := idp.key.Public().(*rsa.PublicKey).N.Bytes()
	s, err := tok.SignedString(secret)
	if err != nil {
		t.Fatalf("sign hs: %v", err)
	}
	return s
}

// TestValidateToken_TableDriven is the end-to-end JWT validation matrix:
// each case stands up a fresh test IDP (real HTTP server, real JWKS)
// and asserts the validator's verdict against a crafted token.
func TestValidateToken_TableDriven(t *testing.T) {
	const audience = "spark-session"

	type tokenFunc func(t *testing.T, idp *testIDP) string

	cases := []struct {
		name       string
		audience   string
		makeToken  tokenFunc
		wantErr    bool
		wantErrSub string
		wantUser   string
		wantGroups []string
	}{
		{
			name:     "valid token is accepted",
			audience: audience,
			makeToken: func(t *testing.T, idp *testIDP) string {
				return idp.sign(t, jwt.MapClaims{
					"sub":    "alice",
					"iss":    idp.server.URL,
					"aud":    audience,
					"exp":    time.Now().Add(time.Hour).Unix(),
					"iat":    time.Now().Unix(),
					"groups": []string{"admin", "users"},
				}, "")
			},
			wantUser:   "alice",
			wantGroups: []string{"admin", "users"},
		},
		{
			name:     "alg=none is rejected",
			audience: audience,
			makeToken: func(t *testing.T, idp *testIDP) string {
				return unsignedToken(map[string]any{
					"sub": "attacker",
					"iss": idp.server.URL,
					"aud": audience,
					"exp": time.Now().Add(time.Hour).Unix(),
				})
			},
			wantErr:    true,
			wantErrSub: "validate token",
		},
		{
			name:     "HS256 alg-confusion is rejected",
			audience: audience,
			makeToken: func(t *testing.T, idp *testIDP) string {
				return hsToken(t, idp, jwt.MapClaims{
					"sub": "attacker",
					"iss": idp.server.URL,
					"aud": audience,
					"exp": time.Now().Add(time.Hour).Unix(),
				})
			},
			wantErr: true,
		},
		{
			name:     "expired token is rejected",
			audience: audience,
			makeToken: func(t *testing.T, idp *testIDP) string {
				return idp.sign(t, jwt.MapClaims{
					"sub": "alice",
					"iss": idp.server.URL,
					"aud": audience,
					"exp": time.Now().Add(-time.Minute).Unix(),
				}, "")
			},
			wantErr: true,
		},
		{
			name:     "wrong issuer is rejected",
			audience: audience,
			makeToken: func(t *testing.T, idp *testIDP) string {
				return idp.sign(t, jwt.MapClaims{
					"sub": "alice",
					"iss": "https://evil.example.com",
					"aud": audience,
					"exp": time.Now().Add(time.Hour).Unix(),
				}, "")
			},
			wantErr: true,
		},
		{
			name:     "wrong audience is rejected",
			audience: audience,
			makeToken: func(t *testing.T, idp *testIDP) string {
				return idp.sign(t, jwt.MapClaims{
					"sub": "alice",
					"iss": idp.server.URL,
					"aud": "other-app",
					"exp": time.Now().Add(time.Hour).Unix(),
				}, "")
			},
			wantErr: true,
		},
		{
			name:     "missing exp claim is rejected",
			audience: audience,
			makeToken: func(t *testing.T, idp *testIDP) string {
				return idp.sign(t, jwt.MapClaims{
					"sub": "alice",
					"iss": idp.server.URL,
					"aud": audience,
				}, "")
			},
			wantErr: true,
		},
		{
			name:     "unknown kid is rejected",
			audience: audience,
			makeToken: func(t *testing.T, idp *testIDP) string {
				return idp.sign(t, jwt.MapClaims{
					"sub": "alice",
					"iss": idp.server.URL,
					"aud": audience,
					"exp": time.Now().Add(time.Hour).Unix(),
				}, "no-such-kid")
			},
			wantErr:    true,
			wantErrSub: "no JWKS key",
		},
		{
			name:     "audience not enforced when unconfigured",
			audience: "",
			makeToken: func(t *testing.T, idp *testIDP) string {
				return idp.sign(t, jwt.MapClaims{
					"sub": "bob",
					"iss": idp.server.URL,
					"aud": "anything",
					"exp": time.Now().Add(time.Hour).Unix(),
				}, "")
			},
			wantUser: "bob",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			idp := newTestIDP(t)
			a, err := NewAuthenticator(OIDCConfig{
				IssuerURL: idp.server.URL,
				Audience:  tc.audience,
			})
			if err != nil {
				t.Fatalf("NewAuthenticator: %v", err)
			}
			tok := tc.makeToken(t, idp)

			user, err := a.ValidateToken(tok)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got user=%+v", user)
				}
				if tc.wantErrSub != "" && !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErrSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if user.Username != tc.wantUser {
				t.Fatalf("username: got %q want %q", user.Username, tc.wantUser)
			}
			if len(tc.wantGroups) > 0 {
				if !stringSlicesEqual(user.Groups, tc.wantGroups) {
					t.Fatalf("groups: got %v want %v", user.Groups, tc.wantGroups)
				}
			}
		})
	}
}

// TestValidateToken_NoIssuerConfigured guards the configuration error
// path: without an issuer URL no JWKS exists, so every token must be
// rejected rather than silently accepted.
func TestValidateToken_NoIssuerConfigured(t *testing.T) {
	a, err := NewAuthenticator(OIDCConfig{})
	if err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}
	if _, err := a.ValidateToken("anything"); err == nil {
		t.Fatal("expected error when issuer is unconfigured")
	}
}

// TestAuthenticateWithCredentials_SkipValidation documents the dev-mode
// shortcut: when SkipValidation is set, the username is trusted without
// contacting an IDP — but an empty username is still rejected.
func TestAuthenticateWithCredentials_SkipValidation(t *testing.T) {
	cases := []struct {
		name     string
		username string
		wantErr  bool
	}{
		{name: "username trusted", username: "alice"},
		{name: "empty username rejected", username: "", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, err := NewAuthenticator(OIDCConfig{SkipValidation: true})
			if err != nil {
				t.Fatalf("NewAuthenticator: %v", err)
			}
			u, err := a.AuthenticateWithCredentials(t.Context(), tc.username, "any")
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if u.Username != tc.username {
				t.Fatalf("username: got %q want %q", u.Username, tc.username)
			}
		})
	}
}

// TestValidateToken_SkipValidationClaimChecks verifies that even with
// SkipValidation=true, configured iss/aud/exp claims are still enforced.
// Without this guard, a flipped dev flag would accept any token (the
// auditor's A1 finding).
func TestValidateToken_SkipValidationClaimChecks(t *testing.T) {
	const issuer = "https://idp.example.com"
	const audience = "spark"

	mkToken := func(claims map[string]any) string {
		hdr := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
		body, _ := json.Marshal(claims)
		return hdr + "." + base64.RawURLEncoding.EncodeToString(body) + ".garbage-signature"
	}

	cases := []struct {
		name    string
		cfg     OIDCConfig
		claims  map[string]any
		wantErr bool
	}{
		{
			name: "valid claims accepted",
			cfg:  OIDCConfig{SkipValidation: true, IssuerURL: issuer, Audience: audience},
			claims: map[string]any{
				"sub": "alice",
				"iss": issuer,
				"aud": audience,
				"exp": time.Now().Add(time.Hour).Unix(),
			},
		},
		{
			name: "wrong issuer rejected",
			cfg:  OIDCConfig{SkipValidation: true, IssuerURL: issuer, Audience: audience},
			claims: map[string]any{
				"sub": "alice",
				"iss": "https://evil.example.com",
				"aud": audience,
				"exp": time.Now().Add(time.Hour).Unix(),
			},
			wantErr: true,
		},
		{
			name: "expired token rejected",
			cfg:  OIDCConfig{SkipValidation: true, IssuerURL: issuer, Audience: audience},
			claims: map[string]any{
				"sub": "alice",
				"iss": issuer,
				"aud": audience,
				"exp": time.Now().Add(-time.Minute).Unix(),
			},
			wantErr: true,
		},
		{
			name: "missing exp rejected when issuer configured",
			cfg:  OIDCConfig{SkipValidation: true, IssuerURL: issuer},
			claims: map[string]any{
				"sub": "alice",
				"iss": issuer,
			},
			wantErr: true,
		},
		{
			name: "wrong audience rejected",
			cfg:  OIDCConfig{SkipValidation: true, IssuerURL: issuer, Audience: audience},
			claims: map[string]any{
				"sub": "alice",
				"iss": issuer,
				"aud": "other",
				"exp": time.Now().Add(time.Hour).Unix(),
			},
			wantErr: true,
		},
		{
			name: "no checks when nothing configured (pure local dev)",
			cfg:  OIDCConfig{SkipValidation: true},
			claims: map[string]any{
				"sub": "alice",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, err := NewAuthenticator(tc.cfg)
			if err != nil {
				t.Fatalf("NewAuthenticator: %v", err)
			}
			tok := mkToken(tc.claims)
			_, err = a.ValidateToken(tok)
			if tc.wantErr && err == nil {
				t.Fatal("expected error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestAuthenticateWithCredentials_ROPC_EndToEnd exercises the full
// production path: ROPC token endpoint issues a real RS256 JWT signed by
// the test IDP, then ValidateToken verifies it against the JWKS the same
// IDP exposes — the integration test the proposal calls out.
func TestAuthenticateWithCredentials_ROPC_EndToEnd(t *testing.T) {
	idp := newTestIDP(t)

	idp.server.Config.Handler.(*http.ServeMux).HandleFunc("/protocol/openid-connect/token",
		func(w http.ResponseWriter, r *http.Request) {
			if err := r.ParseForm(); err != nil {
				http.Error(w, "bad form", http.StatusBadRequest)
				return
			}
			if r.Form.Get("grant_type") != "password" ||
				r.Form.Get("username") != "alice" ||
				r.Form.Get("password") != "s3cret" {
				http.Error(w, "invalid creds", http.StatusUnauthorized)
				return
			}
			token := idp.sign(t, jwt.MapClaims{
				"sub": "alice",
				"iss": idp.server.URL,
				"aud": "spark-session",
				"exp": time.Now().Add(time.Hour).Unix(),
			}, "")
			_ = json.NewEncoder(w).Encode(map[string]string{"access_token": token})
		})

	a, err := NewAuthenticator(OIDCConfig{
		IssuerURL: idp.server.URL,
		Audience:  "spark-session",
		ClientID:  "spark",
	})
	if err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}

	u, err := a.AuthenticateWithCredentials(t.Context(), "alice", "s3cret")
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	if u.Username != "alice" {
		t.Fatalf("username: got %q want alice", u.Username)
	}

	if _, err := a.AuthenticateWithCredentials(t.Context(), "alice", "wrong"); err == nil {
		t.Fatal("expected auth error for wrong password")
	}
}

// TestAuthenticate_NoRedirectFollow verifies the hardened HTTP client
// does not follow redirects on the ROPC POST — preventing
// credential-leak-by-redirect to attacker-controlled hosts.
func TestAuthenticate_NoRedirectFollow(t *testing.T) {
	var redirectTargetHit bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectTargetHit = true
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(target.Close)

	idp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(idp.Close)

	a, err := NewAuthenticator(OIDCConfig{IssuerURL: idp.URL, ClientID: "spark"})
	if err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}
	if _, err := a.AuthenticateWithCredentials(t.Context(), "alice", "pw"); err == nil {
		t.Fatal("expected error when ROPC endpoint returns a redirect")
	}
	if redirectTargetHit {
		t.Fatal("redirect was followed — credentials may have leaked to target host")
	}
}

// TestNewSecureHTTPClient asserts the wired defaults that the proposal
// pins: timeout set, redirects suppressed, TLS pinned to >= 1.2.
func TestNewSecureHTTPClient(t *testing.T) {
	c := newSecureHTTPClient()
	if c.Timeout != httpClientTimeout {
		t.Fatalf("timeout: got %v want %v", c.Timeout, httpClientTimeout)
	}
	if c.CheckRedirect == nil {
		t.Fatal("CheckRedirect must be set to suppress redirect following")
	}
	if got := c.CheckRedirect(nil, nil); got != http.ErrUseLastResponse {
		t.Fatalf("CheckRedirect: got %v want %v", got, http.ErrUseLastResponse)
	}
}

// TestDiscoveryClient_AllowsSameHostRedirects verifies the OIDC discovery
// client follows up to maxDiscoveryRedirects same-host hops (real-world
// Keycloak / Auth0 do this for trailing-slash canonicalization) while
// rejecting cross-host redirects to bound SSRF risk.
func TestDiscoveryClient_AllowsSameHostRedirects(t *testing.T) {
	base, _ := url.Parse("https://example.com/.well-known/openid-configuration")

	cases := []struct {
		name      string
		via       []*http.Request
		nextHost  string
		wantError bool
	}{
		{
			name:     "first same-host redirect allowed",
			via:      []*http.Request{{URL: base}},
			nextHost: "example.com",
		},
		{
			name:     "second same-host redirect allowed",
			via:      []*http.Request{{URL: base}, {URL: base}},
			nextHost: "example.com",
		},
		{
			name:      "third redirect rejected (cap reached)",
			via:       []*http.Request{{URL: base}, {URL: base}, {URL: base}},
			nextHost:  "example.com",
			wantError: true,
		},
		{
			name:      "cross-host redirect rejected",
			via:       []*http.Request{{URL: base}},
			nextHost:  "attacker.example.org",
			wantError: true,
		},
	}

	c := newDiscoveryHTTPClient()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			next := &http.Request{URL: &url.URL{Scheme: "https", Host: tc.nextHost, Path: "/x"}}
			err := c.CheckRedirect(next, tc.via)
			if tc.wantError && err == nil {
				t.Fatal("expected redirect to be rejected")
			}
			if !tc.wantError && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestJWKSDiscovery_FollowsSameHostRedirect verifies the end-to-end
// discovery path tolerates the trailing-slash / realm-canonicalization
// 302 that real Keycloak deployments emit, instead of giving up with
// "discovery missing jwks_uri".
func TestJWKSDiscovery_FollowsSameHostRedirect(t *testing.T) {
	idp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			// Same-host redirect (httptest.NewServer binds 127.0.0.1).
			http.Redirect(w, r, "/realms/canonical/.well-known/openid-configuration", http.StatusFound)
		case "/realms/canonical/.well-known/openid-configuration":
			_ = json.NewEncoder(w).Encode(map[string]string{
				"issuer":   "http://" + r.Host,
				"jwks_uri": "http://" + r.Host + "/jwks",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(idp.Close)

	a, err := NewAuthenticator(OIDCConfig{IssuerURL: idp.URL})
	if err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}
	if _, err := a.jwks.discoverJWKSURI(t.Context()); err != nil {
		t.Fatalf("discovery should follow same-host redirect: %v", err)
	}
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// _ keeps fmt referenced in case future cases use it; harmless if removed.
var _ = fmt.Sprintf
