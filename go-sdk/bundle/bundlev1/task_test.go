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

package bundlev1

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/apache/airflow/go-sdk/pkg/logging"
	"github.com/apache/airflow/go-sdk/sdk"
)

type TaskSuite struct {
	suite.Suite
}

func TestTaskSuite(t *testing.T) {
	suite.Run(t, &TaskSuite{})
}

func (s *TaskSuite) TestReturnValidation() {
	cases := map[string]struct {
		fn          any
		errContains string
	}{
		"no-ret-values": {
			func() {},
			`func\d+ has 0 return values, must be`,
		},
		"too-many-ret-values": {
			func() (a, b, c int) { return },
			`func\d+ has 3 return values, must be`,
		},
		"invalid-ret": {
			func() (c chan int) { return },
			`func\d+ last return value to return error but found chan`,
		},
	}

	for name, tt := range cases {
		s.Run(name, func() {
			_, err := NewTaskFunction(tt.fn)
			if s.Assert().Error(err) {
				s.Assert().Regexp(tt.errContains, err.Error())
			}
		})
	}
}

func (s *TaskSuite) TestArgumentBinding() {
	cases := map[string]struct {
		fn any
	}{
		"no-args": {
			func() error { return nil },
		},
		"context": {
			func(ctx context.Context) error {
				s.Equal("def", ctx.Value("abc"))
				return nil
			},
		},
		"context-and-logger": {
			func(ctx context.Context, logger *slog.Logger) error {
				s.Equal("def", ctx.Value("abc"))
				s.NotNil(logger)
				return nil
			},
		},
		"client": {
			func(client sdk.Client) error {
				s.NotNil(client)
				return nil
			},
		},
		"var-client": {
			func(client sdk.VariableClient) error {
				s.NotNil(client)
				return nil
			},
		},
		"conn-client": {
			func(client sdk.ConnectionClient) error {
				s.NotNil(client)

				return nil
			},
		},
	}

	for name, tt := range cases {
		s.Run(name, func() {
			task, err := NewTaskFunction(tt.fn)
			s.Require().NoError(err)

			ctx := context.WithValue(context.Background(), "abc", "def")
			logger := slog.New(logging.NewTeeLogger())
			task.Execute(ctx, logger)
		})
	}
}
