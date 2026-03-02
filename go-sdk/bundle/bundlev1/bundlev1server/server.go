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

package bundlev1server

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/evanphx/go-hclog-slog/hclogslog"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	flag "github.com/spf13/pflag"

	"github.com/apache/airflow/go-sdk/bundle/bundlev1"
	"github.com/apache/airflow/go-sdk/bundle/bundlev1/bundlev1server/impl"
	"github.com/apache/airflow/go-sdk/pkg/bundles/shared"
	"github.com/apache/airflow/go-sdk/pkg/config"
)

var versionInfo *bool = flag.Bool("bundle-metadata", false, "show the embedded bundle info")

// ServeOpt is an interface for defining options that can be passed to the
// Serve function. Each implementation modifies the ServeConfig being
// generated. A slice of ServeOpts then, cumulatively applied, render a full
// ServeConfig.
type ServeOpt interface {
	ApplyServeOpt(*ServerConfig) error
}

type serveConfigFunc func(*ServerConfig) error

func (s serveConfigFunc) ApplyServeOpt(in *ServerConfig) error {
	return s(in)
}

type ServerConfig struct{}

// Serve is the entrypoint for your bundle, and sets it up ready for Airflow's Go Worker to use
//
// Zero or more options to configure the server may also be passed. There are no options yet, this is to allow
// future changes without breaking compatibility
func Serve(bundle bundlev1.BundleProvider, opts ...ServeOpt) error {
	config.SetupViper("")

	hcLogger := hclog.New(&hclog.LoggerOptions{
		Level:                    hclog.Trace,
		Output:                   os.Stderr,
		JSONFormat:               true,
		IncludeLocation:          true,
		AdditionalLocationOffset: 3,
	})

	log := slog.New(hclogslog.Adapt(hcLogger))
	slog.SetDefault(log)

	flag.Parse()

	serveConfig := &ServerConfig{}
	for _, c := range opts {
		c.ApplyServeOpt(serveConfig)
	}

	if *versionInfo {
		meta := bundle.GetBundleVersion()
		data, err := json.MarshalIndent(meta, "", "    ")
		if err != nil {
			return err
		}
		fmt.Println(string(data))
		return nil
	}

	pluginConfig := &plugin.ServeConfig{
		HandshakeConfig: shared.Handshake,
		Plugins: plugin.PluginSet{
			"dag-bundle": &impl.BundleGRPCPlugin{
				Factory: func() bundlev1.BundleProvider { return bundle },
			},
		},
		GRPCServer: plugin.DefaultGRPCServer,
	}

	// Likely never returns
	plugin.Serve(pluginConfig)

	return nil
}
