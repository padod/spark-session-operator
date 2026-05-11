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

package auth

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/golang-jwt/jwt/v5"
)

// allowedSigningAlgs is the set of JWS algorithms accepted by ValidateToken.
// HMAC algorithms are intentionally excluded to prevent alg-confusion attacks
// where an attacker submits a HS256 token signed with the RSA public key.
var allowedSigningAlgs = []string{"RS256", "ES256"}

// httpClientTimeout is the deadline applied to all outbound auth HTTP calls
// (ROPC, OIDC discovery, JWKS).
const httpClientTimeout = 10 * time.Second

// claimLeeway is the clock-skew slack applied to time-based JWT claims
// (exp / nbf) on the SkipValidation path. The signed validator path uses
// the go-jwt-v5 defaults.
const claimLeeway = 60 * time.Second

// OIDCConfig holds OIDC validation and Keycloak ROPC configuration.
type OIDCConfig struct {
	// IssuerURL is the OIDC issuer URL (e.g. https://keycloak.example.com/realms/spark)
	IssuerURL string
	// Audience expected in the token
	Audience string
	// ClientID is the OAuth client ID for ROPC grant
	ClientID string
	// ClientSecret is the OAuth client secret (optional, depends on Keycloak client config)
	ClientSecret string
	// UserClaim is the JWT claim containing the username (default "sub")
	UserClaim string
	// GroupsClaim is the JWT claim containing user groups (default "groups")
	GroupsClaim string
	// SkipValidation bypasses Keycloak ROPC entirely, trusting the provided username (dev only)
	SkipValidation bool
}

// UserInfo holds user identity extracted from a JWT.
type UserInfo struct {
	Username string
	Groups   []string
}

// maxDiscoveryRedirects bounds the redirect hops the OIDC discovery /
// JWKS client will follow. Real-world Keycloak / Auth0 deployments redirect
// /.well-known/openid-configuration (trailing-slash or realm canonicalization)
// so the strict no-redirect ROPC client cannot be reused for discovery.
// Hops are restricted to the same host to keep SSRF surface bounded.
const maxDiscoveryRedirects = 2

// Authenticator handles JWT validation and Keycloak ROPC token exchange.
type Authenticator struct {
	Config OIDCConfig

	// httpClient is the strict client for credential-bearing POSTs (ROPC):
	// no redirect following, 10s timeout, TLS >= 1.2.
	httpClient *http.Client
	jwks       *jwksCache
}

// NewAuthenticator creates a new Authenticator. The returned instance uses a
// dedicated HTTP client with a 10s timeout, redirect following disabled (to
// avoid SSRF / redirect-driven credential exfiltration on the ROPC POST), and
// TLS minimum version pinned to 1.2. A separate lenient client is used for
// OIDC discovery and JWKS GETs — see newDiscoveryHTTPClient.
//
// The issuer URL must use https unless SkipValidation is set. A plain-http
// issuer would leak the user's password on the ROPC POST and is rejected at
// construction time so misconfiguration is surfaced loudly at startup.
func NewAuthenticator(cfg OIDCConfig) (*Authenticator, error) {
	if cfg.IssuerURL != "" && !cfg.SkipValidation {
		if err := requireHTTPSIssuer(cfg.IssuerURL); err != nil {
			return nil, err
		}
	}
	a := &Authenticator{Config: cfg, httpClient: newSecureHTTPClient()}
	if cfg.IssuerURL != "" {
		a.jwks = newJWKSCache(cfg.IssuerURL, newDiscoveryHTTPClient(), defaultJWKSTTL)
	}
	return a, nil
}

// requireHTTPSIssuer rejects plain-http issuer URLs except when the host is
// a loopback address (127.0.0.0/8, ::1, or the literal "localhost"). Loopback
// traffic does not traverse the network, so the password-leak risk that
// motivates the https requirement on ROPC does not apply — and the test
// suite's httptest IDPs (which bind 127.0.0.1) need to exercise the signed
// JWKS path that SkipValidation deliberately bypasses.
func requireHTTPSIssuer(issuerURL string) error {
	u, err := url.Parse(issuerURL)
	if err != nil {
		return fmt.Errorf("parse issuer URL: %w", err)
	}
	if u.Host == "" {
		return fmt.Errorf("issuer URL missing host")
	}
	if u.Scheme == "https" {
		return nil
	}
	if u.Scheme == "http" && isLoopbackHost(u.Hostname()) {
		return nil
	}
	return fmt.Errorf("issuer URL must use https (got scheme %q); pass --oidc-skip-validation for dev", u.Scheme)
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

func newSecureHTTPClient() *http.Client {
	return &http.Client{
		Timeout: httpClientTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
		},
	}
}

// newDiscoveryHTTPClient returns an HTTP client suitable for OIDC discovery
// and JWKS GETs. Up to maxDiscoveryRedirects same-host redirects are
// followed; cross-host redirects are refused to bound SSRF risk. len(via)
// is the number of requests already made; the upcoming request would be
// hop len(via), so reject once len(via) > maxDiscoveryRedirects.
func newDiscoveryHTTPClient() *http.Client {
	return &http.Client{
		Timeout: httpClientTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) > maxDiscoveryRedirects {
				return fmt.Errorf("stopped after %d redirects", maxDiscoveryRedirects)
			}
			if len(via) > 0 && req.URL.Host != via[0].URL.Host {
				return fmt.Errorf("refused cross-host redirect to %s", req.URL.Host)
			}
			return nil
		},
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
		},
	}
}

// AuthenticateWithCredentials exchanges username/password for a JWT via Keycloak ROPC,
// then extracts UserInfo from the JWT claims. Used by the proxy.
// When SkipValidation is true, bypasses Keycloak entirely and trusts the provided username (dev only).
func (a *Authenticator) AuthenticateWithCredentials(ctx context.Context, username, password string) (*UserInfo, error) {
	if a.Config.SkipValidation {
		if username == "" {
			return nil, fmt.Errorf("username is required even in skip-validation mode")
		}
		return &UserInfo{Username: username}, nil
	}

	token, err := a.exchangeROPC(ctx, username, password)
	if err != nil {
		return nil, err
	}
	return a.ValidateToken(token)
}

func (a *Authenticator) exchangeROPC(ctx context.Context, username, password string) (string, error) {
	tokenURL := strings.TrimRight(a.Config.IssuerURL, "/") + "/protocol/openid-connect/token"

	data := url.Values{
		"grant_type": {"password"},
		"username":   {username},
		"password":   {password},
		"client_id":  {a.Config.ClientID},
	}
	if a.Config.ClientSecret != "" {
		data.Set("client_secret", a.Config.ClientSecret)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return "", fmt.Errorf("create token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("exchange credentials: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("read token response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("credential exchange failed (HTTP %d)", resp.StatusCode)
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return "", fmt.Errorf("parse token response: %w", err)
	}
	if tokenResp.AccessToken == "" {
		return "", fmt.Errorf("no access_token in response")
	}
	return tokenResp.AccessToken, nil
}

// ValidateToken parses a JWT, verifies its signature against the issuer's
// JWKS, and extracts UserInfo. The token is rejected unless:
//   - the JWS algorithm is in allowedSigningAlgs (RS256 or ES256);
//   - the exp claim is present and in the future;
//   - the iss claim equals the configured IssuerURL;
//   - if Audience is configured, the aud claim contains it.
//
// When SkipValidation is true, signature verification is bypassed (dev only)
// but the same iss/exp/aud claim checks still apply when configured — this
// prevents a flipped dev flag from degrading to a blanket "trust any token"
// posture.
func (a *Authenticator) ValidateToken(tokenString string) (*UserInfo, error) {
	if a.Config.SkipValidation {
		return a.parseUnverified(tokenString)
	}
	if a.jwks == nil {
		return nil, fmt.Errorf("OIDC issuer not configured")
	}
	return a.parseAndVerify(tokenString)
}

func (a *Authenticator) parseAndVerify(tokenString string) (*UserInfo, error) {
	opts := []jwt.ParserOption{
		jwt.WithValidMethods(allowedSigningAlgs),
		jwt.WithExpirationRequired(),
		jwt.WithIssuer(strings.TrimRight(a.Config.IssuerURL, "/")),
	}
	if a.Config.Audience != "" {
		opts = append(opts, jwt.WithAudience(a.Config.Audience))
	}

	token, err := jwt.ParseWithClaims(tokenString, jwt.MapClaims{}, a.keyfunc, opts...)
	if err != nil {
		return nil, fmt.Errorf("validate token: %w", err)
	}
	if !token.Valid {
		return nil, fmt.Errorf("token invalid")
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("unexpected claims type")
	}
	return a.extractUserFromClaims(claims)
}

// keyfunc resolves the JWKS key matching the token's `kid` header and
// enforces the per-key algorithm declared in the JWKS document.
func (a *Authenticator) keyfunc(token *jwt.Token) (interface{}, error) {
	kid, _ := token.Header["kid"].(string)
	if kid == "" {
		return nil, fmt.Errorf("token missing kid header")
	}
	ctx, cancel := context.WithTimeout(context.Background(), httpClientTimeout)
	defer cancel()
	k, err := a.jwks.keyForKID(ctx, kid)
	if err != nil {
		return nil, err
	}
	if k.alg != "" && token.Method.Alg() != k.alg {
		return nil, fmt.Errorf("token alg %q does not match JWKS key alg %q", token.Method.Alg(), k.alg)
	}
	return k.pub, nil
}

// parseUnverified is the SkipValidation path: claim extraction without
// signature verification. Documented as dev-only on OIDCConfig.SkipValidation.
// iss / aud / exp claims are still enforced when configured so that a
// flipped flag cannot silently degrade to a blanket "trust any token"
// posture. If neither IssuerURL nor Audience is configured, no claim
// checks are performed (legacy behaviour for purely local dev).
func (a *Authenticator) parseUnverified(tokenString string) (*UserInfo, error) {
	parser := jwt.NewParser()
	token, _, err := parser.ParseUnverified(tokenString, jwt.MapClaims{})
	if err != nil {
		return nil, fmt.Errorf("parse token: %w", err)
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("unexpected claims type")
	}
	if err := a.checkClaims(claims); err != nil {
		return nil, err
	}
	return a.extractUserFromClaims(claims)
}

// checkClaims enforces iss/aud/exp when configured. Used by the
// SkipValidation path so claim-level integrity is preserved even when
// signature verification is bypassed.
func (a *Authenticator) checkClaims(claims jwt.MapClaims) error {
	if a.Config.IssuerURL != "" {
		wantIss := strings.TrimRight(a.Config.IssuerURL, "/")
		gotIss, _ := claims["iss"].(string)
		if strings.TrimRight(gotIss, "/") != wantIss {
			return fmt.Errorf("issuer mismatch")
		}
		if err := claimExpired(claims); err != nil {
			return err
		}
	}
	if a.Config.Audience != "" && !audienceContains(claims["aud"], a.Config.Audience) {
		return fmt.Errorf("audience mismatch")
	}
	return nil
}

func claimExpired(claims jwt.MapClaims) error {
	expUnix, err := unixClaim(claims, "exp")
	if err != nil {
		return err
	}
	if expUnix == 0 {
		return fmt.Errorf("token missing exp claim")
	}
	now := time.Now()
	if now.Add(-claimLeeway).Unix() >= expUnix {
		return fmt.Errorf("token expired")
	}
	nbfUnix, err := unixClaim(claims, "nbf")
	if err != nil {
		return err
	}
	if nbfUnix != 0 && now.Add(claimLeeway).Unix() < nbfUnix {
		return fmt.Errorf("token not yet valid (nbf)")
	}
	return nil
}

// unixClaim extracts a numeric (epoch-seconds) JWT claim. Missing claims
// are reported as (0, nil); only type mismatches return an error.
func unixClaim(claims jwt.MapClaims, name string) (int64, error) {
	raw, ok := claims[name]
	if !ok {
		return 0, nil
	}
	switch v := raw.(type) {
	case float64:
		return int64(v), nil
	case int64:
		return v, nil
	default:
		return 0, fmt.Errorf("token %s claim has unexpected type %T", name, raw)
	}
}

func audienceContains(aud interface{}, want string) bool {
	switch v := aud.(type) {
	case string:
		return v == want
	case []interface{}:
		for _, a := range v {
			if s, ok := a.(string); ok && s == want {
				return true
			}
		}
	}
	return false
}

func (a *Authenticator) extractUserFromClaims(claims jwt.MapClaims) (*UserInfo, error) {
	userClaim := a.Config.UserClaim
	if userClaim == "" {
		userClaim = "sub"
	}
	groupsClaim := a.Config.GroupsClaim
	if groupsClaim == "" {
		groupsClaim = "groups"
	}

	username, ok := claims[userClaim].(string)
	if !ok || username == "" {
		return nil, fmt.Errorf("token missing %s claim", userClaim)
	}
	if err := validateUsername(username); err != nil {
		return nil, err
	}

	var groups []string
	if groupsRaw, ok := claims[groupsClaim]; ok {
		if groupsList, ok := groupsRaw.([]interface{}); ok {
			for _, g := range groupsList {
				if gs, ok := g.(string); ok {
					groups = append(groups, gs)
				}
			}
		}
	}
	return &UserInfo{Username: username, Groups: groups}, nil
}

// validateUsername rejects usernames that would corrupt downstream framing.
// ':' would split the proxied Basic auth (user:pass), CR/LF could smuggle
// HTTP headers, and non-printable bytes are unsafe for backend logs.
func validateUsername(s string) error {
	if strings.ContainsAny(s, ":\r\n\x00") {
		return fmt.Errorf("username contains invalid character")
	}
	for _, r := range s {
		if !unicode.IsPrint(r) {
			return fmt.Errorf("username contains non-printable character")
		}
	}
	return nil
}
