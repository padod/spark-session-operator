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
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	grpcMetadataUsername = "x-spark-username"
	grpcMetadataPassword = "x-spark-password"

	// Spark Connect protobuf field numbers.
	// All request types (ExecutePlan, AnalyzePlan, Config, etc.) have user_context as field 2.
	// UserContext has user_id as field 1.
	protoFieldUserContext protowire.Number = 2
	protoFieldUserID     protowire.Number = 1
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
		return "", "", fmt.Errorf("no credentials in gRPC metadata")
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

// extractCredentialsFromProto extracts credentials from the user_context.user_id field
// of a raw Spark Connect protobuf message.
//
// The user_id value is interpreted as follows:
//   - base64(username:password): decoded and split into username + password (for Keycloak ROPC)
//   - plain string: used as username with empty password (for --oidc-skip-validation mode)
//
// This enables PySpark clients on insecure (non-TLS) channels where gRPC metadata
// credentials are silently dropped. Users set the value via:
//
//	sc://host:port/;user_id=base64(user:pass)   (with ROPC)
//	sc://host:port/;user_id=username             (with skip-validation)
func extractCredentialsFromProto(data []byte) (username, password string, err error) {
	userID, err := extractUserIDFromProto(data)
	if err != nil {
		return "", "", err
	}
	if userID == "" {
		return "", "", fmt.Errorf("empty user_id in request")
	}

	// Try to decode as base64(user:pass)
	decoded, err := base64.StdEncoding.DecodeString(userID)
	if err == nil {
		if idx := strings.IndexByte(string(decoded), ':'); idx >= 0 {
			username = string(decoded[:idx])
			password = string(decoded[idx+1:])
			if username != "" {
				return username, password, nil
			}
		}
	}

	// Plain username (no password)
	return userID, "", nil
}

// extractUserIDFromProto parses user_context.user_id from a raw Spark Connect protobuf message.
// Works for all Spark Connect request types (ExecutePlan, AnalyzePlan, Config, etc.)
// which share the same structure: user_context at field 2, user_id at field 1 within UserContext.
func extractUserIDFromProto(data []byte) (string, error) {
	userCtxBytes, err := extractBytesField(data, protoFieldUserContext)
	if err != nil {
		return "", fmt.Errorf("user_context not found in request: %w", err)
	}

	userID, err := extractBytesField(userCtxBytes, protoFieldUserID)
	if err != nil {
		return "", fmt.Errorf("user_id not found in user_context: %w", err)
	}

	return string(userID), nil
}

// extractBytesField extracts the value of a length-delimited (bytes/string/message) field
// from raw protobuf bytes, without requiring a generated proto schema.
func extractBytesField(data []byte, fieldNum protowire.Number) ([]byte, error) {
	remaining := data
	for len(remaining) > 0 {
		num, typ, n := protowire.ConsumeTag(remaining)
		if n < 0 {
			return nil, fmt.Errorf("invalid protobuf tag")
		}
		remaining = remaining[n:]

		switch typ {
		case protowire.VarintType:
			_, n = protowire.ConsumeVarint(remaining)
		case protowire.Fixed32Type:
			_, n = protowire.ConsumeFixed32(remaining)
		case protowire.Fixed64Type:
			_, n = protowire.ConsumeFixed64(remaining)
		case protowire.BytesType:
			v, vn := protowire.ConsumeBytes(remaining)
			n = vn
			if num == fieldNum {
				return v, nil
			}
		default:
			return nil, fmt.Errorf("unknown wire type %d", typ)
		}

		if n < 0 {
			return nil, fmt.Errorf("invalid protobuf field")
		}
		remaining = remaining[n:]
	}
	return nil, fmt.Errorf("field %d not found", fieldNum)
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

// Name returns "proto" so that gRPC sets the standard content-type
// (application/grpc or application/grpc+proto) when proxying to the backend.
// The codec itself just passes raw bytes through without marshaling.
func (rawCodec) Name() string { return "proto" }

// rawFrame holds raw bytes for transparent gRPC proxying.
type rawFrame struct {
	payload []byte
}
