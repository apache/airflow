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

func (*client) GetConnection(ctx context.Context, connID string) (Connection, error) {
	// TODO: Lookup connection from env var (and handle JSON + URI forms)

	httpClient := ctx.Value(sdkcontext.ApiClientContextKey).(api.ClientInterface)

	resp, err := httpClient.Connections().Get(ctx, connID)
	if err != nil {
		var httpError *api.GeneralHTTPError
		if errors.As(err, &httpError) && httpError.Response.StatusCode() == 404 {
			err = fmt.Errorf("%w: %q", ConnectionNotFound, connID)
		}
		return Connection{}, err
	}

	return connFromAPIResponse(resp)
}

func (c *client) PushXCom(
	ctx context.Context,
	ti api.TaskInstance,
	key string,
	value any,
) error {
	params := api.SetXcomParams{}

	if ti.MapIndex != nil && *ti.MapIndex != -1 {
		params.MapIndex = ti.MapIndex
	}

	httpClient := ctx.Value(sdkcontext.ApiClientContextKey).(api.ClientInterface)
	_, err := httpClient.Xcoms().
		SetResponse(ctx, ti.DagId, ti.RunId, ti.TaskId, key, &params, &value)
	if err != nil {
		return err
	}
	return nil
}

func (*client) GetXCom(
	ctx context.Context,
	dagId, runId, taskId string,
	mapIndex *int,
	key string,
	value any,
) (any, error) {
	params := api.GetXcomParams{
		MapIndex: mapIndex,
	}

	httpClient := ctx.Value(sdkcontext.ApiClientContextKey).(api.ClientInterface)
	res, err := httpClient.Xcoms().Get(ctx, dagId, runId, taskId, key, &params)
	if err != nil {
		var httpError *api.GeneralHTTPError
		if errors.As(err, &httpError) && httpError.Response.StatusCode() == 404 {
			err = fmt.Errorf("%w: %q", XComNotFound, key)
		}
		return nil, err
	}
	// TODO: We probably  want to do some level of xcom deser here
	return res.Value, nil
}
