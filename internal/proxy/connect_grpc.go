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
	"encoding/base64"
	"fmt"
	"strings"

	"google.golang.org/grpc/metadata"
)

const (
	grpcMetadataUsername = "x-spark-username"
	grpcMetadataPassword = "x-spark-password"
)

// extractCredentialsFromGRPCMetadata extracts username and password from gRPC metadata.
// Supports two mechanisms:
//  1. Authorization header with Basic auth: "Basic base64(user:pass)"
//     PySpark sends this via the token param: sc://host:port/;token=base64(user:pass)
//     which sets the "authorization" metadata to "Bearer base64(user:pass)".
//     We accept both "Basic" and "Bearer" schemes with base64(user:pass) payload.
//  2. Explicit x-spark-username / x-spark-password metadata headers (fallback).
func extractCredentialsFromGRPCMetadata(ctx context.Context) (username, password string, err error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", "", fmt.Errorf("no gRPC metadata in context")
	}

	// Try Authorization header first (Basic or Bearer with base64(user:pass))
	if authVals := md.Get("authorization"); len(authVals) > 0 {
		username, password, err = parseBasicAuth(authVals[0])
		if err == nil {
			return username, password, nil
		}
	}

	// Fallback: explicit metadata headers
	userVals := md.Get(grpcMetadataUsername)
	if len(userVals) == 0 {
		return "", "", fmt.Errorf("missing credentials: provide Authorization header (Basic base64(user:pass)) or %s/%s metadata headers", grpcMetadataUsername, grpcMetadataPassword)
	}
	username = userVals[0]

	passVals := md.Get(grpcMetadataPassword)
	if len(passVals) == 0 {
		return "", "", fmt.Errorf("missing %s metadata header", grpcMetadataPassword)
	}
	password = passVals[0]

	if username == "" {
		return "", "", fmt.Errorf("empty %s metadata header", grpcMetadataUsername)
	}

	return username, password, nil
}

// parseBasicAuth parses "Basic base64(user:pass)" or "Bearer base64(user:pass)".
// PySpark's token param sets "Bearer <token>", so we accept both schemes.
func parseBasicAuth(header string) (username, password string, err error) {
	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid authorization header format")
	}

	scheme := strings.ToLower(parts[0])
	if scheme != "basic" && scheme != "bearer" {
		return "", "", fmt.Errorf("unsupported auth scheme: %s", parts[0])
	}

	decoded, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		return "", "", fmt.Errorf("invalid base64 in authorization header: %w", err)
	}

	credentials := string(decoded)
	idx := strings.IndexByte(credentials, ':')
	if idx < 0 {
		return "", "", fmt.Errorf("authorization header missing ':' separator (expected base64(user:pass))")
	}

	username = credentials[:idx]
	password = credentials[idx+1:]

	if username == "" {
		return "", "", fmt.Errorf("empty username in authorization header")
	}

	return username, password, nil
}

// rawCodec implements encoding.Codec for transparent gRPC proxying.
// It passes bytes through without proto marshal/unmarshal.
type rawCodec struct{}

func (rawCodec) Marshal(v interface{}) ([]byte, error) {
	frame, ok := v.(*rawFrame)
	if !ok {
		return nil, fmt.Errorf("rawCodec.Marshal: expected *rawFrame, got %T", v)
	}
	return frame.payload, nil
}

func (rawCodec) Unmarshal(data []byte, v interface{}) error {
	frame, ok := v.(*rawFrame)
	if !ok {
		return fmt.Errorf("rawCodec.Unmarshal: expected *rawFrame, got %T", v)
	}
	frame.payload = data
	return nil
}

func (rawCodec) Name() string { return "raw" }

// rawFrame holds raw bytes for transparent gRPC proxying.
type rawFrame struct {
	payload []byte
}
