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
	"runtime/debug"

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

// sdkModulePath is the import path of the SDK module. Used to identify the
// SDK version from the bundle binary's build info dependencies.
const sdkModulePath = "github.com/apache/airflow/go-sdk"

// Flags. The bundle-metadata flag is the existing ADR 0001 introspection
// hook; --comm and --logs select the coordinator-mode protocol added by
// ADR 0003. All three are read by Serve to choose a server mode below.
var (
	versionInfo = flag.Bool("bundle-metadata", false, "show the embedded bundle info")
	dumpSpec    = flag.Bool(
		"dump-bundle-spec",
		false,
		"print the bundle spec JSON (sdk + dags) used by airflow-go-pack and exit",
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

type ServerConfig struct{}

// serveMode tags the protocol the binary will speak this run.
type serveMode int

const (
	modePlugin       serveMode = iota // go-plugin gRPC (existing Edge Worker path)
	modeMetadataDump                  // --bundle-metadata: print BundleInfo JSON
	modeSpecDump                      // --dump-bundle-spec: print bundle spec JSON (ADR 0002)
	modeCoordinator                   // --comm/--logs: msgpack-over-IPC (ADR 0003)
	modeUsageError                    // misuse: print usage and exit non-zero
)

// Serve is the entrypoint for your bundle, and sets it up ready for Airflow's
// Go Worker (go-plugin) or Python supervisor (coordinator protocol) to use.
//
// The mode is decided from CLI flags and process environment, so user code is
// always one line:
//
//	func main() { bundlev1server.Serve(&myBundle{}) }
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

	switch decideMode() {
	case modeMetadataDump:
		return dumpBundleMetadata(bundle)
	case modeSpecDump:
		return dumpBundleSpec(bundle)
	case modeCoordinator:
		// In coordinator mode the supervisor reads the logs channel for
		// structured records, so configuring the hclog/stderr default
		// logger here is unnecessary — execution.Serve installs its own
		// slog handler against the logs socket before any user code runs.
		return execution.Serve(bundle, *commAddr, *logsAddr)
	case modePlugin:
		installPluginLogger()
		return servePlugin(bundle)
	case modeUsageError:
		fmt.Fprintln(os.Stderr, "error: --comm and --logs must be supplied together")
		flag.CommandLine.SetOutput(os.Stderr)
		flag.Usage()
		os.Exit(2)
	}
	return nil
}

func decideMode() serveMode {
	if *versionInfo {
		return modeMetadataDump
	}
	if *dumpSpec {
		return modeSpecDump
	}
	commSet := *commAddr != ""
	logsSet := *logsAddr != ""
	if commSet && logsSet {
		return modeCoordinator
	}
	if commSet || logsSet {
		// Partial use is a hard error per ADR 0003: both flags are
		// required, otherwise the supervisor is misconfigured and the
		// runtime should fail loudly rather than fall through to
		// go-plugin (which would hang on the missing magic-cookie).
		return modeUsageError
	}
	return modePlugin
}

func dumpBundleMetadata(bundle bundlev1.BundleProvider) error {
	meta := bundle.GetBundleVersion()
	data, err := json.MarshalIndent(meta, "", "    ")
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

// bundleSpec is the wire shape printed by --dump-bundle-spec. The schema is
// stable per ADR 0002 and consumed by airflow-go-pack to populate the
// bundle's airflow-metadata.yaml at build time.
type bundleSpec struct {
	FormatVersion string                   `json:"format_version"`
	SDK           bundleSpecSDK            `json:"sdk"`
	Dags          map[string]bundleSpecDag `json:"dags"`
}

type bundleSpecSDK struct {
	Language string `json:"language"`
	Version  string `json:"version"`
}

type bundleSpecDag struct {
	Tasks []string `json:"tasks"`
}

// dumpBundleSpec runs the bundle's RegisterDags against an in-memory recorder
// and writes the bundle spec JSON to stdout. It must not start the gRPC
// server or contact any external services; the recorder is the only side
// effect.
func dumpBundleSpec(bundle bundlev1.BundleProvider) error {
	reg := bundlev1.New()
	if err := bundle.RegisterDags(reg); err != nil {
		return fmt.Errorf("registering dags: %w", err)
	}

	enum, ok := reg.(bundlev1.EnumerableBundle)
	if !ok {
		return fmt.Errorf("registry does not implement EnumerableBundle")
	}

	spec := bundleSpec{
		FormatVersion: "1.0",
		SDK: bundleSpecSDK{
			Language: "go",
			Version:  sdkVersion(),
		},
		Dags: make(map[string]bundleSpecDag),
	}
	for _, dag := range enum.OrderedDags() {
		taskIDs := make([]string, 0, len(dag.Tasks))
		for _, t := range dag.Tasks {
			taskIDs = append(taskIDs, t.ID)
		}
		spec.Dags[dag.DagID] = bundleSpecDag{Tasks: taskIDs}
	}

	data, err := json.MarshalIndent(spec, "", "    ")
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

// sdkVersion returns the version of the SDK module linked into this binary,
// derived from runtime/debug.ReadBuildInfo. Falls back to "(devel)" when
// build info is unavailable (e.g. tests, bundle binaries built from a local
// replace directive).
func sdkVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "(devel)"
	}
	if info.Main.Path == sdkModulePath && info.Main.Version != "" {
		return info.Main.Version
	}
	for _, dep := range info.Deps {
		if dep.Path == sdkModulePath {
			if dep.Replace != nil && dep.Replace.Version != "" {
				return dep.Replace.Version
			}
			if dep.Version != "" {
				return dep.Version
			}
		}
	}
	return "(devel)"
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
