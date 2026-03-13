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

package sdk

import (
	"encoding/json"
	"net/url"
	"testing"

	"github.com/stretchr/testify/suite"
)

type ConnectionTestSuite struct {
	suite.Suite
}

// helper to create a pointer
func ptr[T any](v T) *T { return &v }

func (suite *ConnectionTestSuite) TestGetURI_TableDriven() {
	tests := map[string]struct {
		conn     Connection
		expected *url.URL
	}{
		"basic scheme and host": {
			conn: Connection{
				Type: "postgres",
				Host: "localhost",
			},
			expected: &url.URL{
				Scheme: "postgres",
				Host:   "localhost",
			},
		},
		"host contains scheme different from type": {
			conn: Connection{
				Type: "postgres",
				Host: "mysql://dbhost",
			},
			expected: &url.URL{
				// Note! This is a departure from Python: That would have had `Host: "mysql://dbhost"` but that is not
				// very useful for _consuming_ the generated URI
				Scheme: "mysql",
				Host:   "dbhost",
			},
		},
		"host contains type as scheme": {
			conn: Connection{
				Type: "postgres",
				Host: "postgres://dbhost",
			},
			expected: &url.URL{
				Scheme: "postgres",
				Host:   "dbhost",
			},
		},
		"with port": {
			conn: Connection{
				Type: "postgres",
				Host: "localhost",
				Port: 5432,
			},
			expected: &url.URL{
				Scheme: "postgres",
				Host:   "localhost:5432",
			},
		},
		"with login and password": {
			conn: Connection{
				Type:     "mysql",
				Host:     "dbhost",
				Login:    ptr("user"),
				Password: ptr("pass"),
			},
			expected: &url.URL{
				Scheme: "mysql",
				Host:   "dbhost",
				User:   url.UserPassword("user", "pass"),
			},
		},
		"with login only": {
			conn: Connection{
				Type:  "mysql",
				Host:  "dbhost",
				Login: ptr("user"),
			},
			expected: &url.URL{
				Scheme: "mysql",
				Host:   "dbhost",
				User:   url.User("user"),
			},
		},
		"with path": {
			conn: Connection{
				Type: "postgres",
				Host: "localhost",
				Path: "dbname",
			},
			expected: &url.URL{
				Scheme: "postgres",
				Host:   "localhost",
				Path:   "/dbname",
			},
		},
		"with extra string param": {
			conn: Connection{
				Type:  "mysql",
				Host:  "dbhost",
				Extra: map[string]any{"foo": "bar"},
			},
			expected: &url.URL{
				Scheme:   "mysql",
				Host:     "dbhost",
				RawQuery: "foo=bar",
			},
		},
		"with extra int, float, bool": {
			conn: Connection{
				Type:  "mysql",
				Host:  "dbhost",
				Extra: map[string]any{"a": 1, "b": "2&5", "c": true},
			},
			expected: &url.URL{
				Scheme:   "mysql",
				Host:     "dbhost",
				RawQuery: "a=1&b=2%265&c=true",
			},
		},
		"with extra nested object": {
			conn: Connection{
				Type:  "mysql",
				Host:  "dbhost",
				Extra: map[string]any{"json": map[string]any{"x": 1}},
			},
			expected: func() *url.URL {
				val, _ := json.Marshal(map[string]any{"x": 1})
				return &url.URL{
					Scheme:   "mysql",
					Host:     "dbhost",
					RawQuery: "json=" + url.QueryEscape(string(val)),
				}
			}(),
		},
		"nil login/password treated distinctly from empty string": {
			conn: Connection{
				Type:     "mysql",
				Host:     "dbhost",
				Login:    nil,
				Password: nil,
			},
			expected: &url.URL{
				Scheme: "mysql",
				Host:   "dbhost",
				User:   nil,
			},
		},
		"empty login, password": {
			conn: Connection{
				Type:     "mysql",
				Host:     "dbhost",
				Login:    ptr(""),
				Password: ptr(""),
			},
			expected: &url.URL{
				Scheme: "mysql",
				Host:   "dbhost",
				User:   url.UserPassword("", ""),
			},
		},
		"no type": {
			conn: Connection{
				Type: "",
				Host: "localhost",
			},
			expected: &url.URL{
				Scheme: "",
				Host:   "localhost",
			},
		},
		"extra empty map": {
			conn: Connection{
				Type:  "mysql",
				Host:  "dbhost",
				Extra: map[string]any{},
			},

			expected: &url.URL{
				Scheme: "mysql",
				Host:   "dbhost",
			},
		},
	}

	for name, tt := range tests {
		suite.Run(name, func() {
			got := tt.conn.GetURI()
			suite.Equal(tt.expected.Scheme, got.Scheme, "scheme")
			suite.Equal(tt.expected.Host, got.Host, "host")
			suite.Equal(tt.expected.Path, got.Path, "path")
			if tt.expected.User != nil {
				suite.NotNil(got.User, "expected user should not be nil")
				suite.Equal(tt.expected.User.String(), got.User.String(), "user")
			} else {
				suite.Nil(got.User, "user should be nil")
			}
			suite.Equal(tt.expected.RawQuery, got.RawQuery, "raw query")
		})
	}
}

func TestConnectionTestSuite(t *testing.T) {
	suite.Run(t, new(ConnectionTestSuite))
}
