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

package cmd

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"

	cc "github.com/ivanpirog/coloredcobra"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
	"github.com/apache/airflow/go-sdk/worker"
)

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "airflow-go-worker",
	Short: "Airflow worker for running Go tasks.",
	Long: `Airflow worker for running Go tasks.

All options (other than ` + "`--config`" + `) can be specified in the config file using
the same name as the CLI argument but without the ` + "`--`" + ` prefix.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return initializeConfig(cmd)
	},
}

// Execute is the main entrypoint, and runs the Celery broker for the given worker
func Execute(worker worker.Worker) {
	// TODO: This should possibly just take a task Registry, not a worker object
	cc.Init(&cc.Config{
		RootCmd:       rootCmd,
		Headings:      cc.Bold,
		Commands:      cc.Yellow + cc.Bold,
		Example:       cc.Italic,
		ExecName:      cc.HiMagenta + cc.Bold,
		Flags:         cc.Green,
		FlagsDataType: cc.Italic + cc.White,
	})
	// Store the worker in the context so we can pull it out later
	ctx := context.WithValue(context.Background(), sdkcontext.WorkerContextKey, worker)
	err := rootCmd.ExecuteContext(ctx)
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "",
		"config file (default is $HOME/airflow/go-sdk.yaml)")
	rootCmd.AddCommand(runCmd)
}

var envKeyReplacer *strings.Replacer = strings.NewReplacer(".", "__", "-", "_")

func setupViper() error {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		airflowHome := os.Getenv("AIRFLOW_HOME")
		if airflowHome == "" {
			home, err := os.UserHomeDir()
			cobra.CheckErr(err)
			airflowHome = path.Join(home, "airflow")
		}

		// Search config in home directory with name ".go-sdk" (without extension).
		viper.AddConfigPath(airflowHome)
		viper.SetConfigType("yaml")
		viper.SetConfigName("go-sdk.yaml")
	}

	viper.SetOptions(viper.ExperimentalBindStruct())

	// Attempt to read the config file, gracefully ignoring errors
	// caused by a config file not being found. Return an error
	// if we cannot parse the config file.
	if err := viper.ReadInConfig(); err != nil {
		// It's okay if there isn't a config file
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return err
		}
	}

	// Make the prefix be AIRFLOW__ -- viper adds an extra `_` automatically
	viper.SetEnvPrefix("AIRFLOW_")
	// Set the key replacer to replace "__" with ".". ie, viper.Get("config.value")
	// looks for the environment variable AIRFLOW__CONFIG__VALUE
	viper.SetEnvKeyReplacer(envKeyReplacer)

	viper.AutomaticEnv() // read in environment variables that match
	return nil
}

// initConfig reads in config file and ENV variables if set.
func initializeConfig(cmd *cobra.Command) error {
	if err := setupViper(); err != nil {
		return err
	}
	// Bind the current command's flags to viper
	bindFlags(cmd, envKeyReplacer)

	return nil
}

// Bind each cobra flag to its associated viper configuration (config file and environment variable)
// This approach cribbed from https://github.com/carolynvs/stingoftheviper/blob/19bd73117f0285436505ca17616cbc394d22e63d/main.go
func bindFlags(cmd *cobra.Command, replacer *strings.Replacer) {
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		// Determine the naming convention of the flags when represented in the config file

		// Since viper does case-insensitive comparisons, we don't need to bother fixing the case, and only need to remove the hyphens.
		configName := replacer.Replace(f.Name)

		// Use cli flags for preference over env or config file!
		if f.Changed {
			// Apply the viper config value to the flag when the flag is not set and viper has a value
			if slice, ok := f.Value.(pflag.SliceValue); ok {
				viper.Set(configName, slice.GetSlice())
			} else {
				viper.Set(configName, f.Value.String())
			}
		} else if viper.IsSet(configName) {
			// If we have a viper config but no flag, set the flag value. This lets `MarkRequiredFlag` work
			val := viper.Get(configName)
			cmd.Flags().Set(f.Name, fmt.Sprintf("%v", val))
		}
	})
}
