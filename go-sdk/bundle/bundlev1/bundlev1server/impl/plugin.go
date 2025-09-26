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

// Package impl contains internal GPRC implementation types.
package impl

// This is in a separate package, so that it doesn't show up to people viewing the docs of bundlev1server, as
// these types and classes aren't relevant to them

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"

	"github.com/google/uuid"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/airflow/go-sdk/bundle/bundlev1"
	proto "github.com/apache/airflow/go-sdk/internal/protov1"
	"github.com/apache/airflow/go-sdk/pkg/api"
	"github.com/apache/airflow/go-sdk/pkg/worker"
)

// BundleGRPCPlugin is an implementation of the github.com/hashicorp/go-plugin#Plugin and
// github.com/hashicorp/go-plugin#GRPCPlugin interfaces, indicating how to
// serve [bundlev1.BundleProvider] as gRPC plugins for go-plugin.
type BundleGRPCPlugin struct {
	plugin.NetRPCUnsupportedPlugin
	Factory func() bundlev1.BundleProvider
}

// Type assertion -- it must be a grpc plugin
var _ plugin.GRPCPlugin = (*BundleGRPCPlugin)(nil)

// Type assertion -- it must be a rpc plugin (even if it just returns errors)
var _ plugin.Plugin = (*BundleGRPCPlugin)(nil)

func (p *BundleGRPCPlugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	impl := p.Factory()
	proto.RegisterDagBundleServer(s, &server{
		Impl: impl,
	})
	return nil
}

// GRPCClient implements plugin.GRPCPlugin.
func (p *BundleGRPCPlugin) GRPCClient(
	ctx context.Context,
	broker *plugin.GRPCBroker,
	c *grpc.ClientConn,
) (interface{}, error) {
	return nil, errors.New("bundlev1server only implements gRPC servers")
}

type server struct {
	sync.RWMutex
	proto.UnimplementedDagBundleServer
	Impl bundlev1.BundleProvider

	bundle bundlev1.Bundle
}

func (g *server) GetMetadata(
	ctx context.Context,
	_ *proto.GetMetadata_Request,
) (*proto.GetMetadata_Response, error) {
	ver := g.Impl.GetBundleVersion()

	info := proto.BundleInfo_builder{
		Name:    &ver.Name,
		Version: ver.Version,
	}.Build()
	resp := proto.GetMetadata_Response_builder{Bundle: info}.Build()

	return resp, nil
}

func (g *server) getCachedBundle(_ context.Context) bundlev1.Bundle {
	g.RWMutex.RLock()
	defer g.RWMutex.RUnlock()
	return g.bundle
}

func (g *server) getBundle(ctx context.Context) (bundlev1.Bundle, error) {
	if b := g.getCachedBundle(ctx); b != nil {
		return b, nil
	}

	g.RWMutex.Lock()
	defer g.RWMutex.Unlock()

	reg := bundlev1.New()
	err := g.Impl.RegisterDags(reg)
	if err != nil {
		return nil, err
	}
	g.bundle = reg

	return g.bundle, err
}

func (g *server) Execute(
	ctx context.Context,
	req *proto.Execute_Request,
) (*proto.Execute_Response, error) {
	if executeTask := req.GetTask(); executeTask != nil {
		return nil, g.executeTask(ctx, executeTask)
	}

	which := req.WhichWorkload().String()
	return nil, status.Errorf(codes.Unimplemented, "Unimplmeneted workload %q", which)
}

func (g *server) executeTask(ctx context.Context, executeTask *proto.ExecuteTaskWorkload) error {
	ti := executeTask.GetTi()
	bundle := executeTask.GetBundleInfo()
	id, err := uuid.Parse(ti.GetId().GetValue())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "unable to parse UUID: %s", err)
	}
	workload := api.ExecuteTaskWorkload{
		Token: executeTask.GetToken(),
		TI: bundlev1.TaskInstance{
			DagId:     ti.GetDagId(),
			Id:        id,
			RunId:     ti.GetRunId(),
			TaskId:    ti.GetTaskId(),
			TryNumber: int(ti.GetTryNumber()),
			// TODO: support otel context carrier
			// ContextCarrier: (map[string]any)(ti.GetOtelContext()),
		},
		BundleInfo: bundlev1.BundleInfo{
			Name: bundle.GetName(),
		},
	}

	if ti.HasMapIndex() {
		idx := int(ti.GetMapIndex())
		workload.TI.MapIndex = &idx
	}

	if bundle.HasVersion() {
		ver := bundle.GetVersion()
		workload.BundleInfo.Version = &ver
	}

	if executeTask.HasLogPath() {
		path := executeTask.GetLogPath()
		workload.LogPath = &path
	}

	dagBundle, err := g.getBundle(ctx)
	if err != nil {
		return status.Errorf(codes.NotFound, "dag bundle not found: %#v", workload.BundleInfo)
	}

	w := worker.NewWithBundle(dagBundle, slog.Default())

	hclog.Default().
		Warn("Does viper work", "url", viper.GetString("execution-api-url"), "env", os.Getenv("AIRFLOW__EXECUTION_API_URL"))
	w, err = w.WithServer(viper.GetString("execution-api-url"))
	if err != nil {
		slog.ErrorContext(ctx, "Error setting ExecutionAPI server for worker", "err", err)
		return err
	}

	return w.ExecuteTaskWorkload(ctx, workload)
}
