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
	"errors"

	"github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"

	"github.com/apache/airflow/go-sdk/bundle/bundlev1"
	proto "github.com/apache/airflow/go-sdk/internal/protov1"
)

type BundleClient interface {
	GetMetadata(ctx context.Context) (bundlev1.GetMetadataResponse, error)

	ExecuteTaskWorkload(ctx context.Context, workload bundlev1.ExecuteTaskWorkload) error
}

// This is the implementation of plugin.GRPCPlugin so we can serve/consume this.
type BundleGRPCPlugin struct {
	// GRPCPlugin must still implement the Plugin interface
	plugin.Plugin
	Impl bundlev1.BundleProvider
}

// Type assertion -- it must be a gprc plugin
var _ plugin.GRPCPlugin = (*BundleGRPCPlugin)(nil)

func ptr[T any](val T) *T {
	return &val
}

func (p *BundleGRPCPlugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	return errors.New("bundlev1 only implements gRPC clients")
}

// GRPCClient implements plugin.GRPCPlugin.
func (p *BundleGRPCPlugin) GRPCClient(
	ctx context.Context,
	broker *plugin.GRPCBroker,
	c *grpc.ClientConn,
) (any, error) {
	return &GRPCClient{client: proto.NewDagBundleClient(c)}, nil
}

// GRPCClient is an implementation of DagBundle that talks over RPC.
type GRPCClient struct{ client proto.DagBundleClient }

func (c *GRPCClient) GetMetadata(ctx context.Context) (bundlev1.GetMetadataResponse, error) {
	ret := bundlev1.GetMetadataResponse{}
	rpcResp, err := c.client.GetMetadata(ctx, proto.GetMetadata_Request_builder{}.Build())
	if err != nil {
		return ret, err
	}

	rpcBundle := rpcResp.GetBundle()
	var version *string = nil
	if rpcBundle.HasVersion() {
		version = ptr(rpcBundle.GetVersion())
	}

	ret.Bundle = bundlev1.BundleInfo{
		Name:    rpcBundle.GetName(),
		Version: version,
	}

	return ret, err
}

func (c *GRPCClient) ExecuteTaskWorkload(
	ctx context.Context,
	workload bundlev1.ExecuteTaskWorkload,
) error {
	tiBuilder := proto.TaskInstance_builder{
		Id:        proto.UUID_builder{Value: ptr(workload.TI.Id.String())}.Build(),
		DagId:     &workload.TI.DagId,
		RunId:     &workload.TI.RunId,
		TaskId:    &workload.TI.TaskId,
		Hostname:  workload.TI.Hostname,
		TryNumber: ptr((int32)(workload.TI.TryNumber)),
		// TODO: OtelContext support!
		// OtelContext: map[string]string{},
	}

	if workload.TI.MapIndex != nil {
		tiBuilder.MapIndex = ptr((int32)(*workload.TI.MapIndex))
	}

	taskWorkloadBuilder := proto.ExecuteTaskWorkload_builder{
		Token: ptr(workload.Token),
		Ti:    tiBuilder.Build(),
		BundleInfo: proto.BundleInfo_builder{
			Name:    &workload.BundleInfo.Name,
			Version: workload.BundleInfo.Version,
		}.Build(),
		LogPath: workload.LogPath,
	}

	req := proto.Execute_Request_builder{
		Task: taskWorkloadBuilder.Build(),
	}.Build()
	_, err := c.client.Execute(ctx, req)
	if err != nil {
		return err
	}

	return nil
}
