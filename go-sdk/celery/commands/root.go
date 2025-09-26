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
	"log/slog"
	"os"

	"github.com/MatusOllah/slogcolor"
	"github.com/fatih/color"
	cc "github.com/ivanpirog/coloredcobra"
	"github.com/spf13/cobra"

	"github.com/apache/airflow/go-sdk/pkg/bundles/shared"
	"github.com/apache/airflow/go-sdk/pkg/logging/shclog"
)

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "airflow-go-celery",
	Short: "Airflow worker for running Go workloads sent via Celery.",
	Long: `Airflow worker for running Go workloads sent via Celery.

All options (other than ` + "`--config`" + `) can be specified in the config file using
the same name as the CLI argument but without the ` + "`--`" + ` prefix.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return initializeConfig(cmd)
	},
}

// Execute is the main entrypoint, and runs the Celery broker app and listens for Celery Tasks
func Execute() {
	cc.Init(&cc.Config{
		RootCmd:       rootCmd,
		Headings:      cc.Bold,
		Commands:      cc.Yellow + cc.Bold,
		Example:       cc.Italic,
		ExecName:      cc.HiMagenta + cc.Bold,
		Flags:         cc.Green,
		FlagsDataType: cc.Italic + cc.White,
	})
	err := rootCmd.ExecuteContext(context.Background())
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "",
		"config file (default is $HOME/airflow/go-sdk.yaml)")
	rootCmd.AddCommand(runCmd)
}

// initConfig reads in config file and ENV variables if set.
func initializeConfig(cmd *cobra.Command) error {
	if err := shared.SetupViper(cfgFile); err != nil {
		return err
	}
	// Bind the current command's flags to viper
	shared.BindFlagsToViper(cmd)

	logger := makeLogger()
	slog.SetDefault(logger)

	return nil
}

func makeLogger() *slog.Logger {
	opts := *slogcolor.DefaultOptions
	leveler := &slog.LevelVar{}
	leveler.Set(shclog.SlogLevelTrace)

	opts.Level = leveler
	opts.LevelTags = map[slog.Level]string{
		shclog.SlogLevelTrace: color.New(color.FgHiGreen).Sprint("TRACE"),
		slog.LevelDebug:       color.New(color.BgCyan, color.FgHiWhite).Sprint("DEBUG"),
		slog.LevelInfo:        color.New(color.BgGreen, color.FgHiWhite).Sprint("INFO "),
		slog.LevelWarn:        color.New(color.BgYellow, color.FgHiWhite).Sprint("WARN "),
		slog.LevelError:       color.New(color.BgRed, color.FgHiWhite).Sprint("ERROR"),
	}

	log := slog.New(slogcolor.NewHandler(os.Stderr, &opts))
	return log
}
