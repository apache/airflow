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
	"errors"
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
	"github.com/apache/airflow/go-sdk/pkg/execution"
)

// ErrCoordinatorFlagsIncomplete is returned by [Serve] when exactly one of
// --comm or --logs is supplied. Both flags select coordinator mode and must
// be set together; callers (typically main) can check for this sentinel to
// print usage before exiting non-zero.
var ErrCoordinatorFlagsIncomplete = errors.New(
	"--comm and --logs must be supplied together",
)

// ErrFormatRequiresMetadata is returned by [Serve] when --format is supplied
// without --airflow-metadata, the only mode whose encoding it selects.
var ErrFormatRequiresMetadata = errors.New(
	"--format is only valid together with --airflow-metadata",
)

// CLI Flags, all read by Serve to choose a server mode below.
// --airflow-metadata prints the bundle's manifest and exits (airflow-go-pack
// consumes it to build the embedded airflow-metadata.yaml); --format selects
// its encoding. --comm and --logs select coordinator mode.
var (
	printMetadata = flag.Bool(
		"airflow-metadata",
		false,
		"print the bundle's airflow-metadata manifest and exit",
	)
	metadataFormat = flag.String(
		"format",
		string(execution.MetadataFormatYAML),
		"encoding for --airflow-metadata: yaml (default) or json; only valid with --airflow-metadata",
	)
	commAddr = flag.String(
		"comm",
		"",
		"host:port of the supervisor's coordinator comm channel (selects coordinator mode)",
	)
	logsAddr = flag.String(
		"logs",
		"",
		"host:port of the supervisor's coordinator logs channel (selects coordinator mode)",
	)
)

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

// ServerConfig holds settings that ServeOpt values apply before the bundle
// server starts. It is currently empty; it exists so options can be added later
// without changing Serve's signature.
type ServerConfig struct{}

// serveMode tags the protocol the binary will speak this run.
type serveMode int

const (
	modePlugin                serveMode = iota // go-plugin gRPC (existing Edge Worker path)
	modeAirflowMetadata                        // --airflow-metadata: print the manifest JSON (ADR 0002/0004)
	modeCoordinator                            // --comm/--logs: msgpack-over-IPC (ADR 0003)
	modeCoordinatorUsageError                  // misuse: print usage and exit non-zero
)

// Serve is the entrypoint for your bundle, and sets it up ready for Airflow's
// Go Worker (go-plugin) or Python supervisor (coordinator protocol) to use.
//
// The mode is decided from CLI flags and process environment. Callers should
// surface the returned error so misuse (e.g. only one of --comm/--logs
// supplied) produces a non-zero exit:
//
//	func main() {
//	    if err := bundlev1server.Serve(&myBundle{}); err != nil {
//	        log.Fatal(err)
//	    }
//	}
//
// Zero or more options to configure the server may also be passed. There are
// no options yet; the parameter exists to allow future additions without
// breaking compatibility.
func Serve(bundle bundlev1.BundleProvider, opts ...ServeOpt) error {
	config.SetupViper("")

	flag.Parse()

	serveConfig := &ServerConfig{}
	for _, c := range opts {
		c.ApplyServeOpt(serveConfig)
	}

	mode := decideMode()

	// --format applies only to --airflow-metadata; reject it elsewhere instead
	// of silently ignoring it.
	if mode != modeAirflowMetadata && flag.CommandLine.Changed("format") {
		return ErrFormatRequiresMetadata
	}

	switch mode {
	case modeAirflowMetadata:
		format, err := execution.ParseMetadataFormat(*metadataFormat)
		if err != nil {
			return err
		}
		return execution.DumpAirflowMetadata(bundle, format)
	case modeCoordinator:
		// In coordinator mode the supervisor reads the logs channel for
		// structured records, so configuring the hclog/stderr default
		// logger here is unnecessary — execution.Serve installs its own
		// slog handler against the logs socket before any user code runs.
		return execution.Serve(bundle, *commAddr, *logsAddr)
	case modePlugin:
		installPluginLogger()
		return servePlugin(bundle)
	case modeCoordinatorUsageError:
		return ErrCoordinatorFlagsIncomplete
	}
	return nil
}

func decideMode() serveMode {
	if *printMetadata {
		return modeAirflowMetadata
	}
	commSet := *commAddr != ""
	logsSet := *logsAddr != ""
	if commSet && logsSet {
		return modeCoordinator
	}
	if commSet || logsSet {
		// Partial use is a hard error, both flags are required
		return modeCoordinatorUsageError
	}
	return modePlugin
}

func installPluginLogger() {
	hcLogger := hclog.New(&hclog.LoggerOptions{
		Level:                    hclog.Trace,
		Output:                   os.Stderr,
		JSONFormat:               true,
		IncludeLocation:          true,
		AdditionalLocationOffset: 3,
	})
	log := slog.New(hclogslog.Adapt(hcLogger))
	slog.SetDefault(log)
}

func servePlugin(bundle bundlev1.BundleProvider) error {
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
