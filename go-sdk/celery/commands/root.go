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
	"os"

	"github.com/spf13/cobra"

	"github.com/apache/airflow/go-sdk/pkg/config"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "airflow-go-celery",
	Short: "Airflow worker for running Go workloads sent via Celery.",
	Long: `Airflow worker for running Go workloads sent via Celery.

All options (other than ` + "`--config`" + `) can be specified in the config file using
the same name as the CLI argument but without the ` + "`--`" + ` prefix.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return config.Configure(cmd)
	},
}

// Execute is the main entrypoint, and runs the Celery broker app and listens for Celery Tasks
func Execute() {
	err := rootCmd.ExecuteContext(context.Background())
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	config.InitColor(rootCmd)
	rootCmd.PersistentFlags().
		String("config", "", "config file (default is $HOME/airflow/go-sdk.yaml)")
	rootCmd.AddCommand(runCmd)
}
