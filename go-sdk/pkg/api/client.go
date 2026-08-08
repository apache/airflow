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
	"fmt"
	"maps"
	"sync/atomic"

	"github.com/google/uuid"
	"resty.dev/v3"
)

const API_VERSION = "2025-05-20"

const refreshedAPITokenHeader = "Refreshed-API-Token"

//go:generate -command openapi-gen go run github.com/ashb/oapi-resty-codegen@latest --config oapi-codegen.yml

//go:generate openapi-gen https://airflow.staged.apache.org/schemas/execution-api/2025-05-20.json

func correlationIdInjector(_ *resty.Client, req *resty.Request) error {
	if uuid, err := uuid.NewV7(); err != nil {
		return err
	} else {
		req.Header.Set("Correlation-Id", uuid.String())
	}
	return nil
}

func NewDefaultClient(server string, opts ...ClientOption) (ClientInterface, error) {
	rc := resty.New()
	rc.SetBaseURL(server)
	rc.SetHeader("Airflow-API-Version", API_VERSION)
	return NewClient(
		server,
		WithClient(rc),
		WithRequestMiddleware(correlationIdInjector),
	)
}

// WithBearerToken creates a copy of the client (reusing the underlying http.Client) adding in a Bearer token auth to all requests
func (c *Client) WithBearerToken(token string) (ClientInterface, error) {
	rc := resty.NewWithClient(c.Client.Client())
	maps.Copy(rc.Header(), c.Client.Header())
	rc.SetBaseURL(c.Server)
	rc.SetDebug(c.Client.IsDebug())
	rc.SetLogger(c.Client.Logger())

	// Keep the token outside Resty's shared header map because task API calls and heartbeats use this client concurrently.
	var authorization atomic.Value
	authorization.Store(fmt.Sprintf("Bearer %s", token))
	rc.AddRequestMiddleware(func(_ *resty.Client, req *resty.Request) error {
		req.Header.Set("Authorization", authorization.Load().(string))
		return nil
	})
	rc.AddResponseMiddleware(func(_ *resty.Client, response *resty.Response) error {
		if refreshedToken := response.Header().Get(refreshedAPITokenHeader); refreshedToken != "" {
			authorization.Store(fmt.Sprintf("Bearer %s", refreshedToken))
		}
		return nil
	})

	opts := []ClientOption{
		WithClient(rc),
	}
	for _, mw := range c.RequestMiddleware {
		opts = append(opts, WithRequestMiddleware(mw))
	}

	return NewClient(c.Server, opts...)
}
