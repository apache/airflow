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

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/google/uuid"
)

const API_VERSION = "2025-05-20"

//go:generate -command openapi-gen go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@latest --config oapi-codegen.yml

//go:generate openapi-gen https://airflow.apache.org/schemas/execution-api/2025-05-20.json

// apiVersionInjector sets the constant Airflow-API-Version header on every request.
func apiVersionInjector(_ context.Context, req *http.Request) error {
	req.Header.Set("Airflow-API-Version", API_VERSION)
	return nil
}

// correlationIdInjector sets a fresh Correlation-Id header on every request.
func correlationIdInjector(_ context.Context, req *http.Request) error {
	id, err := uuid.NewV7()
	if err != nil {
		return err
	}
	req.Header.Set("Correlation-Id", id.String())
	return nil
}

// NewDefaultClient returns a client for the Execution API rooted at server, with
// the API-version and correlation-id request editors installed.
func NewDefaultClient(server string, opts ...ClientOption) (*ClientWithResponses, error) {
	defaults := []ClientOption{
		WithRequestEditorFn(apiVersionInjector),
		WithRequestEditorFn(correlationIdInjector),
	}
	return NewClientWithResponses(server, append(defaults, opts...)...)
}

// WithBearerToken returns a copy of the client (reusing the underlying HTTP doer
// and request editors) that adds a Bearer token to the Authorization header of
// every request. The token is time-limited, so we set the header directly.
func (c *ClientWithResponses) WithBearerToken(token string) (*ClientWithResponses, error) {
	inner, ok := c.ClientInterface.(*Client)
	if !ok {
		return nil, fmt.Errorf(
			"WithBearerToken requires the default *Client, got %T",
			c.ClientInterface,
		)
	}
	authInjector := func(_ context.Context, req *http.Request) error {
		req.Header.Set("Authorization", "Bearer "+token)
		return nil
	}
	return NewClientWithResponses(
		inner.Server,
		WithHTTPClient(inner.Client),
		func(nc *Client) error {
			nc.RequestEditors = append(nc.RequestEditors, inner.RequestEditors...)
			nc.RequestEditors = append(nc.RequestEditors, authInjector)
			return nil
		},
	)
}

// GeneralHTTPError is returned when the Execution API responds with a non-2xx
// status. It carries the underlying response so callers can inspect the status
// code and the originating request.
type GeneralHTTPError struct {
	Response *http.Response
	// JSON holds the decoded error body when it is a JSON object; otherwise Text
	// holds the raw body.
	JSON any
	Text string
}

func (e *GeneralHTTPError) Error() string {
	if e.Text != "" {
		return fmt.Sprintf("%s: %s", e.Response.Status, e.Text)
	}
	return e.Response.Status
}

// apiResponse is implemented by every generated typed response wrapper.
type apiResponse interface {
	StatusCode() int
	GetBody() []byte
}

// CheckResponse returns a *GeneralHTTPError when resp represents a non-2xx
// response, and nil otherwise. rawResponse is the underlying *http.Response,
// which the generated wrapper carries as its HTTPResponse field.
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
