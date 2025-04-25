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
	"io"
	"net/http"

	"github.com/google/uuid"
	"github.com/oapi-codegen/oapi-codegen/v2/pkg/securityprovider"
)

func apiVersionInjector(ctx context.Context, req *http.Request) error {
	req.Header.Set("Airflow-API-Version", API_VERSION)
	return nil
}

func correlationIdInjector(ctx context.Context, req *http.Request) error {
	if uuid, err := uuid.NewV7(); err != nil {
		return err
	} else {
		req.Header.Set("Correlation-Id", uuid.String())
	}
	return nil
}

func NewDefaultClient(server string, opts ...ClientOption) (*Client, error) {
	return NewClient(
		server, WithRequestEditorFn(apiVersionInjector), WithRequestEditorFn(correlationIdInjector),
	)
}

// WithBearerToken creates a copy of the client adding in a Bearer token security provider
func (c *Client) WithBearerToken(token string) (*Client, error) {
	auth, err := securityprovider.NewSecurityProviderBearerToken(token)
	if err != nil {
		return nil, err
	}

	opts := []ClientOption{WithRequestEditorFn(auth.Intercept)}

	if c.Client != nil {
		opts = append(opts, WithHTTPClient(c.Client))
	}
	for _, fn := range c.RequestEditors {
		opts = append(opts, WithRequestEditorFn(fn))
	}
	return NewClient(c.Server, opts...)
}

func (*Client) ResponseErrorToJson(resp *http.Response) (any, error) {
	if resp.Body == nil {
		return "Unknown error", nil
	}
	b, err := io.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		return nil, err
	}

	// Try to json decode it, else just return the content
	var jsonMap map[string](any)

	err = json.Unmarshal(b, &jsonMap)
	if err != nil {
		return string(b), nil
	}
	return jsonMap, nil
}
