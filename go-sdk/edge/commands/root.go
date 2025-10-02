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
	"github.com/spf13/cobra"

	"github.com/apache/airflow/go-sdk/pkg/config"
)

// Root represents the base command when called without any subcommands
var Root = &cobra.Command{
	Use:   "airflow-go-edge",
	Short: "Airflow worker for running Go workloads sent via Edge Worker API.",
	Long: `Airflow worker for running Go workloads sent via Edge Worker API.

All options (other than ` + "`--config`" + `) can be specified in the config file using
the same name as the CLI argument but without the ` + "`--`" + ` prefix.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return config.Configure(cmd)
	},
	SilenceUsage: true,
}

func init() {
	Root.PersistentFlags().
		String("config", "", "config file (default is $HOME/airflow/go-sdk.yaml)")
	Root.AddCommand(runCmd)
	config.InitColor(Root)
}
