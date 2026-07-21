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

package bundlev1client

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/apache/airflow/go-sdk/bundle/bundlev1"
	proto "github.com/apache/airflow/go-sdk/internal/protov1"
	"github.com/apache/airflow/go-sdk/pkg/api"
)

type recordingDagBundleClient struct {
	request *proto.Execute_Request
}

func (*recordingDagBundleClient) GetMetadata(
	context.Context,
	*proto.GetMetadata_Request,
	...grpc.CallOption,
) (*proto.GetMetadata_Response, error) {
	return proto.GetMetadata_Response_builder{}.Build(), nil
}

func (c *recordingDagBundleClient) Execute(
	_ context.Context,
	request *proto.Execute_Request,
	_ ...grpc.CallOption,
) (*proto.Execute_Response, error) {
	c.request = request
	return proto.Execute_Response_builder{}.Build(), nil
}

func TestExecuteTaskWorkloadPropagatesOTelContext(t *testing.T) {
	contextCarrier := map[string]any{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"tracestate":  "vendor=value",
		"non-string":  42,
	}
	recorder := &recordingDagBundleClient{}
	client := &GRPCClient{client: recorder}

	err := client.ExecuteTaskWorkload(t.Context(), bundlev1.ExecuteTaskWorkload{
		TI: api.TaskInstance{
			Id:             uuid.New(),
			ContextCarrier: &contextCarrier,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, recorder.request)
	assert.Equal(t, map[string]string{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"tracestate":  "vendor=value",
	}, recorder.request.GetTask().GetTi().GetOtelContext())
}
