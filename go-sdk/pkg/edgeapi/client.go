// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package edgeapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/apache/airflow/go-sdk/pkg/config"
)

const edgeAPIPathPrefix = "/edge_worker/v1/"

//go:generate -command openapi-gen go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@latest --config oapi-codegen.yml

//go:generate openapi-gen ../../../providers/edge3/src/airflow/providers/edge3/worker_api/v2-edge-generated.yaml

// NewDefaultClient returns an Edge worker API client rooted at server.
func NewDefaultClient(server string, opts ...ClientOption) (*ClientWithResponses, error) {
	return NewClientWithResponses(server, opts...)
}

// WithEdgeAPIJWTKey installs a request editor that signs every request with a
// short-lived HS512 JWT whose "method" claim is the endpoint path. The Edge API
// server authenticates each request against this per-endpoint token.
func WithEdgeAPIJWTKey(key []byte, issuer string) ClientOption {
	editor := func(_ context.Context, req *http.Request) error {
		endpointPath := req.URL.Path
		if idx := strings.Index(endpointPath, edgeAPIPathPrefix); idx >= 0 {
			endpointPath = endpointPath[idx+len(edgeAPIPathPrefix):]
		}
		now := time.Now().UTC().Unix()
		token := jwt.NewWithClaims(jwt.SigningMethodHS512, jwt.MapClaims{
			"method": endpointPath,
			"iss":    issuer,
			"aud":    "api",
			"iat":    now,
			"nbf":    now,
			"exp":    now + 5,
		})
		signed, err := token.SignedString(key)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", signed)
		return nil
	}
	return WithRequestEditorFn(editor)
}

// WithRetry installs an HTTP transport that retries transient failures (temporary
// or timed-out network errors, connection refused, and HTTP 502) up to
// conf.RetryCount times, backing off between conf.StartWaitTime and
// conf.MaxWaitTime.
func WithRetry(conf config.ClientConfig) ClientOption {
	return func(c *Client) error {
		base := http.DefaultTransport
		if hc, ok := c.Client.(*http.Client); ok && hc.Transport != nil {
			base = hc.Transport
		}
		c.Client = &http.Client{Transport: &retryTransport{
			base:    base,
			count:   conf.RetryCount,
			waitMin: conf.StartWaitTime,
			waitMax: conf.MaxWaitTime,
		}}
		return nil
	}
}

type retryTransport struct {
	base    http.RoundTripper
	count   int
	waitMin time.Duration
	waitMax time.Duration
}

func (t *retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Buffer the body so it can be replayed on each retry.
	var body []byte
	if req.Body != nil {
		var err error
		if body, err = io.ReadAll(req.Body); err != nil {
			return nil, err
		}
		_ = req.Body.Close()
	}

	var resp *http.Response
	var err error
	for attempt := 0; ; attempt++ {
		if body != nil {
			req.Body = io.NopCloser(bytes.NewReader(body))
		}
		resp, err = t.base.RoundTrip(req)
		if attempt >= t.count || !shouldRetry(resp, err) {
			return resp, err
		}
		slog.Warn("Retrying Edge API request", "attempt", attempt+1, "error", err)
		wait := t.waitMin * time.Duration(1<<attempt)
		if wait > t.waitMax || wait <= 0 {
			wait = t.waitMax
		}
		select {
		case <-time.After(wait):
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	}
}

func shouldRetry(resp *http.Response, err error) bool {
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if opErr.Temporary() || opErr.Timeout() {
			return true
		}
		var sysErr *os.SyscallError
		if errors.As(opErr.Err, &sysErr) && errors.Is(sysErr.Err, syscall.ECONNREFUSED) {
			return true
		}
	}
	return resp != nil && resp.StatusCode == http.StatusBadGateway
}

// GeneralHTTPError is returned when the Edge API responds with a non-2xx status.
type GeneralHTTPError struct {
	Response *http.Response
	JSON     any
	Text     string
}

func (e *GeneralHTTPError) Error() string {
	if e.Text != "" {
		return fmt.Sprintf("%s: %s", e.Response.Status, e.Text)
	}
	return e.Response.Status
}

type apiResponse interface {
	StatusCode() int
	GetBody() []byte
}

// CheckResponse returns a *GeneralHTTPError when resp is a non-2xx response.
func CheckResponse(resp apiResponse, rawResponse *http.Response) error {
	if code := resp.StatusCode(); code >= 200 && code < 300 {
		return nil
	}
	e := GeneralHTTPError{Response: rawResponse, Text: string(resp.GetBody())}
	if rawResponse != nil && rawResponse.Header.Get("Content-Type") == "application/json" {
		if json.Unmarshal(resp.GetBody(), &e.JSON) == nil {
			e.Text = ""
		}
	}
	return &e
}
