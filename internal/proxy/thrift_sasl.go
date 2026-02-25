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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

// SASL frame status codes used by HiveServer2 Thrift SASL transport.
const (
	saslStart    byte = 0x01
	saslOK       byte = 0x02
	saslBAD      byte = 0x03
	saslERROR    byte = 0x04
	saslComplete byte = 0x05
)

// ThriftSASLAuth holds credentials extracted from the SASL PLAIN handshake.
type ThriftSASLAuth struct {
	Username string
	Password string
}

// readSASLFrame reads a single SASL frame: 1-byte status + 4-byte big-endian length + payload.
// Returns the status, payload, the complete raw bytes read, and any error.
func readSASLFrame(r io.Reader) (status byte, payload []byte, raw []byte, err error) {
	header := make([]byte, 5)
	if _, err = io.ReadFull(r, header); err != nil {
		return 0, nil, nil, fmt.Errorf("read SASL header: %w", err)
	}

	status = header[0]
	length := binary.BigEndian.Uint32(header[1:5])

	payload = make([]byte, length)
	if length > 0 {
		if _, err = io.ReadFull(r, payload); err != nil {
			return 0, nil, nil, fmt.Errorf("read SASL payload: %w", err)
		}
	}

	raw = append(header, payload...)
	return status, payload, raw, nil
}

// writeSASLFrame writes a single SASL frame to w.
func writeSASLFrame(w io.Writer, status byte, payload []byte) error {
	header := make([]byte, 5)
	header[0] = status
	binary.BigEndian.PutUint32(header[1:5], uint32(len(payload)))

	if _, err := w.Write(header); err != nil {
		return fmt.Errorf("write SASL header: %w", err)
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return fmt.Errorf("write SASL payload: %w", err)
		}
	}
	return nil
}

// ExtractThriftSASLAuth reads the first two SASL frames from a client connection
// to extract the username and password from a SASL PLAIN handshake.
//
// Frame 1 (START): mechanism name, must be "PLAIN"
// Frame 2 (OK): payload is \x00username\x00password (RFC 4616)
//
// Returns the auth info and all raw bytes read (for replay to the backend).
func ExtractThriftSASLAuth(conn net.Conn) (*ThriftSASLAuth, []byte, error) {
	var allRaw []byte

	// Frame 1: START with mechanism
	status, payload, raw, err := readSASLFrame(conn)
	if err != nil {
		return nil, nil, fmt.Errorf("read SASL START frame: %w", err)
	}
	allRaw = append(allRaw, raw...)

	if status != saslStart {
		return nil, nil, fmt.Errorf("expected SASL START (0x01), got 0x%02x", status)
	}
	mechanism := string(payload)
	if mechanism != "PLAIN" {
		return nil, nil, fmt.Errorf("unsupported SASL mechanism: %s (only PLAIN supported)", mechanism)
	}

	// Frame 2: OK with credentials
	status, payload, raw, err = readSASLFrame(conn)
	if err != nil {
		return nil, nil, fmt.Errorf("read SASL credentials frame: %w", err)
	}
	allRaw = append(allRaw, raw...)

	if status != saslOK {
		return nil, nil, fmt.Errorf("expected SASL OK (0x02), got 0x%02x", status)
	}

	// RFC 4616: payload = [authzid] \x00 authcid \x00 passwd
	parts := bytes.SplitN(payload, []byte{0x00}, 3)
	if len(parts) != 3 {
		return nil, nil, fmt.Errorf("invalid SASL PLAIN payload: expected 3 parts, got %d", len(parts))
	}

	username := string(parts[1])
	password := string(parts[2])

	if username == "" {
		return nil, nil, fmt.Errorf("empty username in SASL PLAIN payload")
	}

	return &ThriftSASLAuth{Username: username, Password: password}, allRaw, nil
}

// CompleteSASLHandshake sends a SASL COMPLETE frame to the client to finish the handshake.
func CompleteSASLHandshake(conn net.Conn) error {
	return writeSASLFrame(conn, saslComplete, nil)
}
