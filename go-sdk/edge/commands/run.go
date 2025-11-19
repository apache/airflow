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

package commands

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/apache/airflow/go-sdk/edge"
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Connect to Edge Executor API and run Airflow workloads",
	Long:  "Connect to Edge Executor API and run Airflow workloads",

	RunE: func(cmd *cobra.Command, args []string) error {
		return edge.Run(context.Background())
	},
}

func init() {
	flags := runCmd.Flags()
	flags.StringP(
		"execution-api-url",
		"",
		"http://localhost:8080/execution/",
		"Execution API to connect to",
	)
	flags.StringSliceP(
		"queues",
		"q",
		[]string{"default"},
		"Comma delimited list of queues to serve, serve all queues if not provided.",
	)
	flags.StringP(
		"api-url",
		"",
		"",
		"URL endpoint on which the Airflow code edge API is accessible from edge worker.",
	)
	flags.StringP(
		"hostname",
		"H",
		"",
		"Set the hostname of worker if you have multiple workers on a single machine.",
	)

	runCmd.MarkFlagRequired("api-url")
	flags.SetAnnotation("api-url", "viper-mapping", []string{"edge.api_url"})
	flags.SetAnnotation("hostname", "viper-mapping", []string{"edge.hostname"})
}
