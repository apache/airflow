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

package main

import (
	"fmt"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/apache/airflow/go-sdk/internal/bundlefooter"
)

func newInspectCmd() *cobra.Command {
	var showSource bool
	cmd := &cobra.Command{
		Use:   "inspect <bundle>",
		Short: "Print the manifest (and optionally source files) embedded in a bundle",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			source, manifest, err := bundlefooter.Read(args[0])
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			if showSource {
				files, err := embeddedSources(source, manifest)
				if err != nil {
					return err
				}
				for _, f := range files {
					fmt.Fprintf(out, "# --- source: %s ---\n", f.path)
					out.Write(f.data)
					if len(f.data) > 0 && f.data[len(f.data)-1] != '\n' {
						fmt.Fprintln(out)
					}
				}
				fmt.Fprintln(out, "# --- manifest ---")
			}
			out.Write(manifest)
			if len(manifest) > 0 && manifest[len(manifest)-1] != '\n' {
				fmt.Fprintln(out)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&showSource, "source", false, "also print each embedded source file")
	return cmd
}

type embeddedSource struct {
	path string
	data []byte
}

// embeddedSources cuts the source region into the files the manifest's sources index lists.
func embeddedSources(region, manifest []byte) ([]embeddedSource, error) {
	var index struct {
		Sources []struct {
			Path   string `yaml:"path"`
			Offset int    `yaml:"offset"`
			Length int    `yaml:"length"`
		} `yaml:"sources"`
	}
	if err := yaml.Unmarshal(manifest, &index); err != nil {
		return nil, fmt.Errorf("decoding manifest: %w", err)
	}
	if len(index.Sources) == 0 && len(region) > 0 {
		return nil, fmt.Errorf(
			"the manifest lists no sources for the %d-byte source region; "+
				"repack the bundle with this airflow-go-pack",
			len(region),
		)
	}
	files := make([]embeddedSource, 0, len(index.Sources))
	for _, src := range index.Sources {
		if src.Offset < 0 || src.Length < 0 || src.Offset > len(region)-src.Length {
			return nil, fmt.Errorf(
				"manifest source %q (offset %d, length %d) does not fit the %d-byte source region",
				src.Path, src.Offset, src.Length, len(region),
			)
		}
		files = append(
			files,
			embeddedSource{path: src.Path, data: region[src.Offset : src.Offset+src.Length]},
		)
	}
	return files, nil
}
