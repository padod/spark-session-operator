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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// defaultJWKSTTL is the lifetime of a cached JWKS document before it is
// re-fetched from the issuer. A short TTL keeps key rotation responsive
// without hammering the IDP on every token validation.
const defaultJWKSTTL = 15 * time.Minute

// minRSAKeyBits is the smallest RSA modulus we trust for token signature
// verification. NIST SP 800-57 deprecates < 2048-bit RSA; weaker keys are
// computationally forgeable.
const minRSAKeyBits = 2048

// jwksKey is a parsed signing key from a JWKS document.
type jwksKey struct {
	kid string
	alg string
	pub any // *rsa.PublicKey or *ecdsa.PublicKey
}

// jwksCache fetches and caches the issuer's JWKS keys with a fixed TTL.
// Lookups by kid that miss the cache (either expired or unknown kid) trigger
// a coalesced refresh — concurrent misses share a single in-flight fetch
// instead of serialising behind a long-held mutex.
type jwksCache struct {
	issuerURL string
	client    *http.Client
	ttl       time.Duration

	mu        sync.RWMutex
	keys      map[string]jwksKey
	jwksURI   string
	fetchedAt time.Time

	refresh singleflight.Group
}

func newJWKSCache(issuerURL string, client *http.Client, ttl time.Duration) *jwksCache {
	return &jwksCache{
		issuerURL: strings.TrimRight(issuerURL, "/"),
		client:    client,
		ttl:       ttl,
		keys:      map[string]jwksKey{},
	}
}

// keyForKID returns the parsed public key with the given kid, refreshing the
// cache when the TTL has elapsed or the kid is unknown. HTTP refresh is run
// outside the lock so concurrent token validations are not head-of-line
// blocked on a slow IDP.
func (c *jwksCache) keyForKID(ctx context.Context, kid string) (jwksKey, error) {
	if k, ok := c.lookupFresh(kid); ok {
		return k, nil
	}
	if err := c.refreshOnce(ctx); err != nil {
		return jwksKey{}, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	k, ok := c.keys[kid]
	if !ok {
		return jwksKey{}, fmt.Errorf("no JWKS key for kid %q", kid)
	}
	return k, nil
}

// lookupFresh returns the cached key if the cache is still within its TTL.
func (c *jwksCache) lookupFresh(kid string) (jwksKey, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.fetchedAt.IsZero() || time.Since(c.fetchedAt) >= c.ttl {
		return jwksKey{}, false
	}
	k, ok := c.keys[kid]
	return k, ok
}

// refreshOnce coalesces concurrent refresh callers via singleflight: only
// one HTTP fetch is in flight at a time; the rest wait for its result.
func (c *jwksCache) refreshOnce(ctx context.Context) error {
	_, err, _ := c.refresh.Do("jwks", func() (any, error) {
		return nil, c.doRefresh(ctx)
	})
	return err
}

func (c *jwksCache) doRefresh(ctx context.Context) error {
	uri, err := c.resolvedJWKSURI(ctx)
	if err != nil {
		return err
	}
	keys, err := c.fetchKeys(ctx, uri)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.jwksURI = uri
	c.keys = keys
	c.fetchedAt = time.Now()
	c.mu.Unlock()
	return nil
}

// resolvedJWKSURI returns the cached discovery URI if known, otherwise it
// discovers and validates it against the configured issuer host.
func (c *jwksCache) resolvedJWKSURI(ctx context.Context) (string, error) {
	c.mu.RLock()
	cached := c.jwksURI
	c.mu.RUnlock()
	if cached != "" {
		return cached, nil
	}
	return c.discoverJWKSURI(ctx)
}

func (c *jwksCache) discoverJWKSURI(ctx context.Context) (string, error) {
	u := c.issuerURL + "/.well-known/openid-configuration"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", fmt.Errorf("create discovery request: %w", err)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetch discovery: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("discovery returned HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("read discovery: %w", err)
	}
	var doc struct {
		JWKSURI string `json:"jwks_uri"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return "", fmt.Errorf("parse discovery: %w", err)
	}
	if doc.JWKSURI == "" {
		return "", fmt.Errorf("discovery missing jwks_uri")
	}
	if err := validateJWKSURI(c.issuerURL, doc.JWKSURI); err != nil {
		return "", err
	}
	return doc.JWKSURI, nil
}

// validateJWKSURI guards against SSRF / JWKS-rebinding by requiring the
// discovery-declared jwks_uri to share the issuer's host and use https
// (http is permitted only when the issuer itself is plain http — i.e.
// local dev — so the validator never weakens an https issuer config).
func validateJWKSURI(issuerURL, jwksURI string) error {
	iss, err := url.Parse(issuerURL)
	if err != nil {
		return fmt.Errorf("parse issuer URL: %w", err)
	}
	got, err := url.Parse(jwksURI)
	if err != nil {
		return fmt.Errorf("parse jwks_uri: %w", err)
	}
	if got.Host != iss.Host {
		return fmt.Errorf("jwks_uri host %q does not match issuer host %q", got.Host, iss.Host)
	}
	const schemeHTTP = "http"
	if got.Scheme != "https" && (iss.Scheme != schemeHTTP || got.Scheme != schemeHTTP) {
		return fmt.Errorf("jwks_uri scheme %q is not https", got.Scheme)
	}
	return nil
}

func (c *jwksCache) fetchKeys(ctx context.Context, uri string) (map[string]jwksKey, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return nil, fmt.Errorf("create JWKS request: %w", err)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch JWKS: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("JWKS returned HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read JWKS: %w", err)
	}
	return parseJWKS(body)
}

// rawJWK is the JSON shape of a single key entry in a JWKS document.
type rawJWK struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	N   string `json:"n"`
	E   string `json:"e"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

func parseJWKS(body []byte) (map[string]jwksKey, error) {
	var doc struct {
		Keys []rawJWK `json:"keys"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("parse JWKS: %w", err)
	}
	out := make(map[string]jwksKey, len(doc.Keys))
	for _, k := range doc.Keys {
		if k.Use != "" && k.Use != "sig" {
			continue
		}
		pub, alg, err := parseJWK(k)
		if err != nil {
			continue
		}
		out[k.Kid] = jwksKey{kid: k.Kid, alg: alg, pub: pub}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no usable JWKS keys")
	}
	return out, nil
}

func parseJWK(k rawJWK) (any, string, error) {
	switch k.Kty {
	case "RSA":
		return parseRSAJWK(k)
	case "EC":
		return parseECJWK(k)
	default:
		return nil, "", fmt.Errorf("unsupported kty %q", k.Kty)
	}
}

func parseRSAJWK(k rawJWK) (any, string, error) {
	n, err := base64URLBigInt(k.N)
	if err != nil {
		return nil, "", fmt.Errorf("RSA n: %w", err)
	}
	e, err := base64URLBigInt(k.E)
	if err != nil {
		return nil, "", fmt.Errorf("RSA e: %w", err)
	}
	if !e.IsInt64() || e.Int64() <= 0 {
		return nil, "", fmt.Errorf("RSA e out of range")
	}
	if n.BitLen() < minRSAKeyBits {
		return nil, "", fmt.Errorf("RSA key too small: %d bits (minimum %d)", n.BitLen(), minRSAKeyBits)
	}
	pub := &rsa.PublicKey{N: n, E: int(e.Int64())}
	alg := k.Alg
	if alg == "" {
		alg = "RS256"
	}
	return pub, alg, nil
}

func parseECJWK(k rawJWK) (any, string, error) {
	var curve elliptic.Curve
	switch k.Crv {
	case "P-256":
		curve = elliptic.P256()
	case "P-384":
		curve = elliptic.P384()
	case "P-521":
		curve = elliptic.P521()
	default:
		return nil, "", fmt.Errorf("unsupported EC curve %q", k.Crv)
	}
	x, err := base64URLBigInt(k.X)
	if err != nil {
		return nil, "", fmt.Errorf("EC x: %w", err)
	}
	y, err := base64URLBigInt(k.Y)
	if err != nil {
		return nil, "", fmt.Errorf("EC y: %w", err)
	}
	pub := &ecdsa.PublicKey{Curve: curve, X: x, Y: y}
	alg := k.Alg
	if alg == "" {
		alg = "ES256"
	}
	return pub, alg, nil
}

func base64URLBigInt(s string) (*big.Int, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	return new(big.Int).SetBytes(b), nil
}
