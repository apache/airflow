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
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"resty.dev/v3"

	"github.com/apache/airflow/go-sdk/pkg/api"
	apiMock "github.com/apache/airflow/go-sdk/pkg/api/mocks"
	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
)

type ClientSuite struct {
	suite.Suite

	apiClient         *apiMock.ClientInterface
	variablesClient   *apiMock.VariablesClient
	connectionsClient *apiMock.ConnectionsClient
	ctx               context.Context
}

var AnyContext any = mock.MatchedBy(func(_ context.Context) bool { return true })

func makeHTTPError(status int, statusMessage string) error {
	return &api.GeneralHTTPError{
		Response: &resty.Response{
			RawResponse: &http.Response{
				Status:     fmt.Sprintf("%d %s", status, statusMessage),
				StatusCode: status,
			},
		},
	}
}

func TestClientSuite(t *testing.T) {
	suite.Run(t, &ClientSuite{})
}

func (s *ClientSuite) SetupTest() {
	c := apiMock.NewClientInterface(s.T())
	vars := apiMock.NewVariablesClient(s.T())
	conns := apiMock.NewConnectionsClient(s.T())
	c.EXPECT().Variables().Maybe().Return(vars)
	c.EXPECT().Connections().Maybe().Return(conns)

	s.apiClient = c
	s.variablesClient = vars
	s.connectionsClient = conns
	s.ctx = context.WithValue(context.Background(), sdkcontext.ApiClientContextKey, c)
}

func (s *ClientSuite) TestGetVariable() {
	key := "my_var"
	expected := `some"raw"value`
	s.variablesClient.EXPECT().
		Get(AnyContext, key).
		Return(&api.VariableResponse{Value: &expected}, nil)

	c := &client{}
	val, err := c.GetVariable(s.ctx, key)
	s.Require().NoError(err)
	s.Equal(expected, val)
}

func (s *ClientSuite) TestGetVariable_404Error() {
	key := "my_var"
	s.variablesClient.EXPECT().Get(AnyContext, key).Return(nil, makeHTTPError(404, "Not Found"))

	c := &client{}
	_, err := c.GetVariable(s.ctx, key)
	s.ErrorContainsf(err, `variable not found: "my_var"`, "")
}

func (s *ClientSuite) TestGetVariable_EnvFirst() {
	s.T().Setenv("AIRFLOW_VAR_MY_VAR", "value1")

	c := &client{}
	val, err := c.GetVariable(s.ctx, "my_var")
	s.Require().NoError(err)
	s.Equal("value1", val)
	s.variablesClient.AssertNotCalled(s.T(), "Get")
}

func (s *ClientSuite) TestGetConnection() {
	tests := map[string]struct {
		connId      string
		apiResponse *api.ConnectionResponse
		wantConn    Connection
		wantErr     bool
	}{
		"minimal fields": {
			connId: "my-db",
			apiResponse: &api.ConnectionResponse{
				ConnId:   "my-db",
				ConnType: "postgres",
			},
			wantConn: Connection{
				ID:   "my-db",
				Type: "postgres",
			},
		},
		"all fields set": {
			connId: "full-db",
			apiResponse: &api.ConnectionResponse{
				ConnId:   "full-db",
				ConnType: "mysql",
				Host:     ptr("dbhost"),
				Port:     ptr(3306),
				Login:    ptr("user"),
				Password: ptr("pass"),
				Schema:   ptr("schema"),
				Extra:    ptr(`{"foo": "bar", "baz": 42}`),
			},
			wantConn: Connection{
				ID:       "full-db",
				Type:     "mysql",
				Host:     "dbhost",
				Port:     3306,
				Login:    ptr("user"),
				Password: ptr("pass"),
				Path:     "schema",
				Extra: map[string]any{
					"foo": "bar",
					"baz": float64(42),
				},
			},
		},
		"extra field not valid json": {
			connId: "bad-extra",
			apiResponse: &api.ConnectionResponse{
				ConnId:   "bad-extra",
				ConnType: "mysql",
				Extra:    ptr(`{"foo": "bar"`), // malformed JSON
			},
			wantConn: Connection{
				ID:    "bad-extra",
				Type:  "mysql",
				Extra: map[string]any{},
			},
			wantErr: true,
		},
		"nil host/port/schema/extra": {
			connId: "nil-fields",
			apiResponse: &api.ConnectionResponse{
				ConnId:   "nil-fields",
				ConnType: "sqlite",
			},
			wantConn: Connection{
				ID:   "nil-fields",
				Type: "sqlite",
			},
		},
		"empty login/password": {
			connId: "empty-login",
			apiResponse: &api.ConnectionResponse{
				ConnId:   "empty-login",
				ConnType: "mysql",
				Login:    ptr(""),
				Password: ptr(""),
			},
			wantConn: Connection{
				ID:       "empty-login",
				Type:     "mysql",
				Login:    ptr(""),
				Password: ptr(""),
			},
		},
	}

	for name, tc := range tests {
		s.Run(name, func() {
			s.connectionsClient.EXPECT().
				Get(AnyContext, tc.connId).
				Return(tc.apiResponse, nil)

			c := &client{}
			gotConn, err := c.GetConnection(s.ctx, tc.connId)
			if tc.wantErr {
				s.Error(err)
			} else {
				s.NoError(err)
			}
			s.Equal(tc.wantConn, gotConn)
		})
	}
}
