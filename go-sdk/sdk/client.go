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

/*
Package sdk provides access to the Airflow objects (Variables, Connection, XCom etc) during run time for tasks.
*/
package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/apache/airflow/go-sdk/pkg/api"
	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
)

type client struct{}

var _ Client = (*client)(nil)

func NewClient() Client {
	return &client{}
}

func variableFromEnv(key string) (string, bool) {
	return os.LookupEnv(VariableEnvPrefix + strings.ToUpper(key))
}

func (*client) GetVariable(ctx context.Context, key string) (string, error) {
	// TODO: Let the lookup priority be configurable like it is in Python SDK
	if env, ok := variableFromEnv(key); ok {
		return env, nil
	}

	httpClient := ctx.Value(sdkcontext.ApiClientContextKey).(api.ClientInterface)

	resp, err := httpClient.Variables().Get(ctx, key)
	if err != nil {
		var httpError *api.GeneralHTTPError
		errors.As(err, &httpError)
		if errors.As(err, &httpError) && httpError.Response.StatusCode() == 404 {
			err = fmt.Errorf("%w: %q", VariableNotFound, key)
		}
		return "", err
	}
	return *resp.Value, nil
}

// UnmarshalJSONVariable implements AirflowClient.
func (c *client) UnmarshalJSONVariable(ctx context.Context, key string, pointer any) error {
	val, err := c.GetVariable(ctx, key)
	if err != nil {
		return err
	}

	return json.Unmarshal([]byte(val), pointer)
}
