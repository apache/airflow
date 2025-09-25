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
	"log/slog"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/apache/airflow/go-sdk/celery"
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Connect to Celery broker and run Airflow workloads",
	Long:  "Connect to Celery broker and run Airflow workloads",

	RunE: func(cmd *cobra.Command, args []string) error {
		var config celery.Config

		viper.BindPFlags(cmd.Flags())

		err := viper.Unmarshal(&config)
		cobra.CheckErr(err)

		err = celery.Run(cmd.Context(), config)
		if err != nil {
			slog.Error("program stopped", "error", err)
			os.Exit(1)
		}
		return nil
	},
}

func init() {
	runCmd.Flags().StringP("broker-address", "b", "", "Celery Broker host:port to connect to")
	runCmd.Flags().
		StringP("execution-api-url", "e", "http://localhost:8080/execution/", "Execution API to connect to")
	runCmd.Flags().StringSliceP("queues", "q", []string{"default"}, "Celery queues to listen on")
	runCmd.Flags().
		StringP("bundles-folder", "", "", "Folder containing the compiled dag bundle executables")

	runCmd.MarkFlagRequired("broker-address")
	runCmd.MarkFlagRequired("bundles-folder")
	runCmd.Flags().
		SetAnnotation("broker-address", "viper-mapping", []string{"celery.broker-address"})
	runCmd.Flags().SetAnnotation("bundles-folder", "viper-mapping", []string{"bundles.folder"})
}
