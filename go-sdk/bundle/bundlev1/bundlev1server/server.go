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

	flag "github.com/spf13/pflag"

	"github.com/apache/airflow/go-sdk/bundle/bundlev1"
	"github.com/apache/airflow/go-sdk/pkg/execution"
)

// ErrCoordinatorFlagsRequired is returned by [Serve] unless both --comm and
// --logs are supplied. Bundle execution always uses the coordinator protocol.
var ErrCoordinatorFlagsRequired = errors.New(
	"--comm and --logs are required for bundle execution",
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
	modeAirflowMetadata       serveMode = iota // --airflow-metadata: print the manifest JSON (ADR 0002/0004)
	modeCoordinator                            // --comm/--logs: msgpack-over-IPC (ADR 0003)
	modeCoordinatorUsageError                  // missing coordinator flags
)

// Serve is the entrypoint for a bundle executed by Airflow's coordinator.
//
// The mode is decided from CLI flags. Callers should
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
	flag.Parse()

	serveConfig := &ServerConfig{}
	for _, c := range opts {
		c.ApplyServeOpt(serveConfig)
	}

	mode := decideMode(*printMetadata, *commAddr, *logsAddr)

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
		return execution.Serve(bundle, *commAddr, *logsAddr)
	case modeCoordinatorUsageError:
		return ErrCoordinatorFlagsRequired
	}
	return nil
}

func decideMode(metadata bool, comm, logs string) serveMode {
	if metadata {
		return modeAirflowMetadata
	}
	commSet := comm != ""
	logsSet := logs != ""
	if commSet && logsSet {
		return modeCoordinator
	}
	return modeCoordinatorUsageError
}
