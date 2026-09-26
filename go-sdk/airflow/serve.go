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

package airflow

import (
	"errors"
	"fmt"
	"io"
	"os"

	flag "github.com/spf13/pflag"

	"github.com/apache/airflow/go-sdk/pkg/execution"
)

// errCoordinatorFlagsRequired is returned by Serve unless both --comm and
// --logs are supplied. Bundle execution always uses the coordinator protocol.
var errCoordinatorFlagsRequired = errors.New(
	"--comm and --logs are required for bundle execution",
)

// errFormatRequiresMetadata is returned by Serve when --format is supplied
// without --airflow-metadata, the only mode whose encoding it selects.
var errFormatRequiresMetadata = errors.New(
	"--format is only valid together with --airflow-metadata",
)

// serveMode tags the protocol the binary will speak this run.
type serveMode int

const (
	modeAirflowMetadata       serveMode = iota // --airflow-metadata: print the manifest JSON (ADR 0002/0004)
	modeCoordinator                            // --comm/--logs: msgpack-over-IPC (ADR 0003)
	modeCoordinatorUsageError                  // missing coordinator flags
)

// Serve runs the bundle. Call it as the last statement of main.
//
// Serve closes registration: a [BundleRef.Register] call that reaches the bundle afterwards
// panics rather than changing what the running bundle answers for.
//
// The command-line flags of the executable decide what Serve does.
// With --airflow-metadata it prints the bundle's manifest and returns, which is how
// airflow-go-pack reads the registered Dag and task ids.
// With --comm and --logs, which the Airflow supervisor passes, it runs one task over the
// coordinator protocol.
//
// main must exit with a non-zero status when Serve returns an error, because the exit status
// is how the supervisor learns that the task failed:
//
//	if err := bundle.Serve(); err != nil {
//		log.Fatal(err)
//	}
func (b *BundleRef) Serve() error {
	return b.serve(os.Args[1:], os.Stdout)
}

func (b *BundleRef) serve(args []string, stdout io.Writer) error {
	// Registration closes here, whatever this run turns out to do, so that a Register left
	// below Serve in main is reported as the mistake it is rather than racing the runtime.
	b.closed.Store(true)

	// The flags go on their own FlagSet. On pflag.CommandLine, every program that imports this
	// package would get them, and one that defines its own --format there would panic.
	flags := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	// --help is output the caller asked for, so it goes to stdout. Anything else pflag prints,
	// such as a deprecation warning for a flag the bundle defines, stays on stderr where it
	// cannot land in the middle of the --airflow-metadata manifest.
	flags.Usage = func() {
		fmt.Fprintf(stdout, "Usage of %s:\n%s", flags.Name(), flags.FlagUsages())
	}
	printMetadata := flags.Bool(
		"airflow-metadata",
		false,
		"print the bundle's airflow-metadata manifest and exit",
	)
	metadataFormat := flags.String(
		"format",
		string(execution.MetadataFormatYAML),
		"encoding for --airflow-metadata: yaml (default) or json; only valid with --airflow-metadata",
	)
	commAddr := flags.String(
		"comm",
		"",
		"host:port of the supervisor's coordinator comm channel (selects coordinator mode)",
	)
	logsAddr := flags.String(
		"logs",
		"",
		"host:port of the supervisor's coordinator logs channel (selects coordinator mode)",
	)
	// A bundle may define flags of its own on pflag.CommandLine. Serve parses the whole command
	// line, so it has to accept those too. AddFlagSet skips a flag whose name is already taken,
	// so Serve checks the names first. Otherwise it would ignore the bundle's flag and report
	// nothing.
	var taken string
	flags.VisitAll(func(f *flag.Flag) {
		if taken == "" && flag.CommandLine.Lookup(f.Name) != nil {
			taken = f.Name
		}
	})
	if taken != "" {
		return fmt.Errorf("the bundle defines a --%s flag, but Serve reserves that name", taken)
	}
	flags.AddFlagSet(flag.CommandLine)
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	mode := decideMode(*printMetadata, *commAddr, *logsAddr)

	// --format applies only to --airflow-metadata; reject it elsewhere instead
	// of silently ignoring it.
	if mode != modeAirflowMetadata && flags.Changed("format") {
		return errFormatRequiresMetadata
	}

	switch mode {
	case modeAirflowMetadata:
		format, err := execution.ParseMetadataFormat(*metadataFormat)
		if err != nil {
			return err
		}
		return execution.DumpAirflowMetadata(stdout, &b.taskHandlers, format)
	case modeCoordinator:
		return execution.Serve(&b.taskHandlers, *commAddr, *logsAddr)
	case modeCoordinatorUsageError:
		return errCoordinatorFlagsRequired
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
