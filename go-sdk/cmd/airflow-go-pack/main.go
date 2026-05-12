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

// Command airflow-go-pack builds a self-contained Airflow bundle from a Go
// package. It runs `go build`, exec's the freshly built binary with
// `--dump-bundle-spec` to obtain the manifest, and appends the source plus
// manifest plus AFBNDL01 trailer to the executable as specified by ADR 0004.
//
// Usage:
//
//	go tool airflow-go-pack [./path/to/pkg] [-- <go build flags>...]
//	go tool airflow-go-pack --executable ./build/example --source main.go
//	go tool airflow-go-pack inspect ./mybundle
//
// See go-sdk/adr/0002-use-go-tool-directive-for-bundle-packer.md and
// go-sdk/adr/0004-self-contained-executable-bundle.md.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func main() {
	if err := newRootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	opts := &packOptions{}

	root := &cobra.Command{
		Use:   "airflow-go-pack [package]",
		Short: "Build a self-contained Airflow bundle from a Go package",
		Long: `airflow-go-pack builds a Go bundle binary, queries it for its DAG/task
identity via --dump-bundle-spec, and appends the source plus an
airflow-metadata.yaml manifest plus an AFBNDL01 trailer to the
executable. The result is a single self-contained file that drops into
[executable] bundles_folder.

By default the packer builds the package in the current directory. Pass
a different package as the positional argument; pass extra go build
flags after a "--" separator.

Examples:
  go tool airflow-go-pack
  go tool airflow-go-pack ./cmd/my-bundle -- -trimpath -tags=prod
  go tool airflow-go-pack --executable ./build/example --source main.go
`,
		// Only count args BEFORE "--" toward the positional limit; args
		// after "--" are forwarded verbatim to `go build` and must not
		// inflate the count (e.g. `-- -ldflags "-X main.foo=bar"`).
		Args: func(cmd *cobra.Command, args []string) error {
			dashAt := cmd.ArgsLenAtDash()
			pkgArgs := args
			if dashAt >= 0 {
				pkgArgs = args[:dashAt]
			}
			return cobra.MaximumNArgs(1)(cmd, pkgArgs)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			// Anything after "--" is forwarded to the internal `go build`
			// invocation. ArgsLenAtDash() returns the count of args before
			// the dash, or -1 if the dash isn't present.
			dashAt := cmd.ArgsLenAtDash()
			var pkgArgs, buildArgs []string
			if dashAt < 0 {
				pkgArgs = args
			} else {
				pkgArgs = args[:dashAt]
				buildArgs = args[dashAt:]
			}
			opts.pkg = "."
			if len(pkgArgs) == 1 {
				opts.pkg = pkgArgs[0]
			}
			opts.buildArgs = buildArgs
			return runPack(cmd.OutOrStdout(), cmd.ErrOrStderr(), opts)
		},
	}

	root.Flags().StringVar(&opts.source, "source",
		"",
		"path to the DAG source file (defaults to the file in the target package containing func main)")
	root.Flags().StringVar(&opts.executable, "executable",
		"",
		"pack a pre-built executable instead of running go build")
	root.Flags().StringVar(&opts.output, "output",
		"",
		"output bundle path (defaults to ./<bundleName>)")

	root.AddCommand(newInspectCmd())
	return root
}
