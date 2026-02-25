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

	"google.golang.org/grpc/metadata"
)

const (
	grpcMetadataUsername = "x-spark-username"
	grpcMetadataPassword = "x-spark-password"
)

// extractCredentialsFromGRPCMetadata extracts username and password from gRPC metadata headers.
// Users set these via spark.connect.grpc.metadata config:
//   - x-spark-username: the user's domain username
//   - x-spark-password: the user's domain password
func extractCredentialsFromGRPCMetadata(ctx context.Context) (username, password string, err error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", "", fmt.Errorf("no gRPC metadata in context")
	}

	userVals := md.Get(grpcMetadataUsername)
	if len(userVals) == 0 {
		return "", "", fmt.Errorf("missing %s metadata header", grpcMetadataUsername)
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
