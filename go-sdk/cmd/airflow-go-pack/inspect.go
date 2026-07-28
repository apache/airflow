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

	"github.com/apache/airflow/go-sdk/internal/bundlefooter"
)

func newInspectCmd() *cobra.Command {
	var showSource bool
	cmd := &cobra.Command{
		Use:   "inspect <bundle>",
		Short: "Print the manifest (and optionally source) embedded in a bundle",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			source, manifest, err := bundlefooter.Read(args[0])
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			if showSource {
				fmt.Fprintln(out, "# --- source ---")
				out.Write(source)
				if len(source) > 0 && source[len(source)-1] != '\n' {
					fmt.Fprintln(out)
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
	cmd.Flags().BoolVar(&showSource, "source", false, "also print the embedded source file")
	return cmd
}
