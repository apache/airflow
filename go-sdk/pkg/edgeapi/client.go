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
	"errors"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"resty.dev/v3"

	"github.com/apache/airflow/go-sdk/pkg/config"
)

//go:generate -command openapi-gen go run github.com/ashb/oapi-resty-codegen@latest --config oapi-codegen.yml

//go:generate openapi-gen https://raw.githubusercontent.com/apache/airflow/refs/tags/providers-edge3/1.3.0/providers/edge3/src/airflow/providers/edge3/worker_api/v2-edge-generated.yaml

func WithEdgeAPIJWTKey(key []byte, issuer string) ClientOption {
	return func(c *Client) error {
		c.SetAuthScheme("")

		mw := func(c *resty.Client, req *resty.Request) error {
			endpointPath := strings.TrimPrefix(req.RawRequest.URL.String(), c.BaseURL())
			endpointPath = strings.TrimPrefix(endpointPath, "/edge_worker/v1/")
			now := time.Now().UTC().Unix()
			t := jwt.NewWithClaims(jwt.SigningMethodHS512, jwt.MapClaims{
				"method": endpointPath,
				"iss":    issuer,
				"aud":    "api",
				"iat":    now,
				"nbf":    now,
				"exp":    now + 5,
			})
			s, err := t.SignedString(key)
			if err != nil {
				return err
			}
			req.RawRequest.Header.Set(
				req.HeaderAuthorizationKey,
				strings.TrimSpace(req.AuthScheme+" "+s),
			)
			return nil
		}

		mws := append(c.RequestMiddleware, resty.PrepareRequestMiddleware, mw)
		c.SetRequestMiddlewares(mws...)
		return nil
	}
}

func WithRetry(conf config.ClientConfig) ClientOption {
	return func(c *Client) error {
		c.SetRetryCount(conf.RetryCount).
			SetRetryWaitTime(conf.StartWaitTime).
			SetRetryMaxWaitTime(conf.MaxWaitTime).
			AddRetryConditions(func(r *resty.Response, err error) bool {
				var opErr *net.OpError

				if errors.As(err, &opErr) {
					if opErr.Temporary() || opErr.Timeout() {
						c.Logger().Warnf("Retrying request %v", err)
						return true
					}
					if sysErr, ok := opErr.Err.(*os.SyscallError); ok {
						if sysErr.Err == syscall.ECONNREFUSED {
							c.Logger().Warnf("Retrying request %v", err)
							return true
						}
					}
				}
				if r.StatusCode() == http.StatusBadGateway {
					c.Logger().Warnf("Retrying request %v", err)
					return true
				}
				return false
			}).SetAllowNonIdempotentRetry(true)
		return nil
	}
}
