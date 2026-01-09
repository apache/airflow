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
	"strconv"
	"strings"

	"github.com/apache/airflow/go-sdk/pkg/api"
)

type Connection struct {
	ID string

	// The Connection type, as defined in Airflow
	Type string

	Host string
	Port int

	// Login/username of the connection. Optional, can be nil (nil is an indication of no login which is distinct from an empty login)
	Login *string
	// Password of the connection. Optional, can be nil.
	Password *string

	// The path. Called `Schema` in Airflow python code. For database connections this often contains the
	// database name.
	Path string

	Extra map[string]any
}

func (c Connection) GetURI() *url.URL {
	uri := &url.URL{}

	if c.Type != "" {
		uri.Scheme = c.Type
	}
	if strings.Contains(c.Host, "://") {
		parts := strings.SplitN(c.Host, "://", 2)
		uri.Host = parts[1]

		// Different protocol, add it to the URI
		if parts[0] != c.Type {
			uri.Scheme = parts[0]
		}
	} else {
		uri.Host = c.Host
	}

	// Host (with port) -- not a separate field on URL object
	if c.Port != 0 {
		uri.Host = uri.Host + ":" + strconv.Itoa(c.Port)
	}

	// Authority block (login:password@)
	if c.Login != nil {
		if c.Password != nil {
			uri.User = url.UserPassword(*c.Login, *c.Password)
		} else {
			uri.User = url.User(*c.Login)
		}
	}

	if c.Path != "" {
		uri.Path = "/" + c.Path
	}

	// Extra/query parameters
	if len(c.Extra) > 0 {
		values := url.Values{}
		for k, v := range c.Extra {
			switch val := v.(type) {
			case string:
				values.Set(k, val)
			case int:
				values.Set(k, strconv.Itoa(val))
			case float64:
				values.Set(k, strconv.FormatFloat(val, 'f', -1, 64))
			case bool:
				values.Set(k, strconv.FormatBool(val))
			default:
				// For nested objects/arrays, marshal as JSON string
				jsonVal, err := json.Marshal(val)
				if err == nil {
					values.Set(k, string(jsonVal))
				}
			}
		}
		uri.RawQuery = values.Encode()
	}

	return uri
}

func connFromAPIResponse(resp *api.ConnectionResponse) (Connection, error) {
	var err error
	conn := Connection{
		ID:       resp.ConnId,
		Type:     resp.ConnType,
		Host:     "",
		Port:     0,
		Login:    resp.Login,
		Password: resp.Password,
	}
	if resp.Host != nil {
		conn.Host = *resp.Host
	}
	if resp.Port != nil {
		conn.Port = *resp.Port
	}
	if resp.Schema != nil {
		conn.Path = *resp.Schema
	}
	if resp.Extra != nil {
		conn.Extra = map[string]any{}
		err = json.Unmarshal([]byte(*resp.Extra), &conn.Extra)
	}
	return conn, err
}
