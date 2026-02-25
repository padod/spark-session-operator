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

package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

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

// Authenticator handles JWT validation and Keycloak ROPC token exchange.
type Authenticator struct {
	Config OIDCConfig
}

// NewAuthenticator creates a new Authenticator.
func NewAuthenticator(cfg OIDCConfig) *Authenticator {
	return &Authenticator{Config: cfg}
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
		return nil, fmt.Errorf("failed to create token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to exchange credentials: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read token response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("credential exchange failed (HTTP %d): %s", resp.StatusCode, string(body))
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return nil, fmt.Errorf("failed to parse token response: %w", err)
	}
	if tokenResp.AccessToken == "" {
		return nil, fmt.Errorf("no access_token in response")
	}

	return a.ValidateToken(tokenResp.AccessToken)
}

// ValidateToken parses a JWT and extracts UserInfo.
// TODO: add JWKS signature verification for production use.
func (a *Authenticator) ValidateToken(tokenString string) (*UserInfo, error) {
	parser := jwt.NewParser()
	token, _, err := parser.ParseUnverified(tokenString, jwt.MapClaims{})
	if err != nil {
		return nil, fmt.Errorf("failed to parse token: %w", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("unexpected claims type")
	}

	return a.extractUserFromClaims(claims)
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
