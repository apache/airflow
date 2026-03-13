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

package config

import (
	"fmt"
	"log/slog"
	"os"
	"path"
	"strings"

	"github.com/MatusOllah/slogcolor"
	"github.com/fatih/color"
	cc "github.com/ivanpirog/coloredcobra"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/apache/airflow/go-sdk/pkg/logging"
)

type BundleConfig struct {
	BundlesFolder string `mapstructure:"bundles_folder"`
}

var envKeyReplacer *strings.Replacer = strings.NewReplacer(".", "__", "-", "_")

func InitColor(rootCmd *cobra.Command) {
	cc.Init(&cc.Config{
		RootCmd:       rootCmd,
		Headings:      cc.Bold,
		Commands:      cc.Yellow + cc.Bold,
		Example:       cc.Italic,
		ExecName:      cc.HiMagenta + cc.Bold,
		Flags:         cc.Green,
		FlagsDataType: cc.Italic + cc.White,
	})
}

func Configure(cmd *cobra.Command) error {
	var cfgFile string
	cfgFlag := cmd.Flags().Lookup("config")
	if cfgFlag != nil {
		cfgFile = cfgFlag.Value.String()
	}

	v, err := SetupViper(cfgFile)
	if err != nil {
		return err
	}
	// Bind the current command's flags to viper
	BindFlagsToViper(cmd, v)

	logger := makeLogger(v)
	slog.SetDefault(logger)

	return nil
}

func makeLogger(v *viper.Viper) *slog.Logger {
	opts := *slogcolor.DefaultOptions
	leveler := &slog.LevelVar{}

	// TODO: Should we have consistency with Airflow's config option? That would mean "logging.logging_level" here
	levelConfig := v.GetString("log.level")

	switch strings.ToUpper(levelConfig) {
	case "TRACE":
		leveler.Set(logging.LevelTrace)
	case "":
		// Default level is info. Job done
	default:
		err := leveler.UnmarshalText([]byte(levelConfig))
		cobra.CheckErr(err)
	}

	opts.Level = leveler
	opts.LevelTags = map[slog.Level]string{
		logging.LevelTrace: color.New(color.FgHiGreen).Sprint("TRACE"),
		slog.LevelDebug:    color.New(color.BgCyan, color.FgHiWhite).Sprint("DEBUG"),
		slog.LevelInfo:     color.New(color.BgGreen, color.FgHiWhite).Sprint("INFO "),
		slog.LevelWarn:     color.New(color.BgYellow, color.FgHiWhite).Sprint("WARN "),
		slog.LevelError:    color.New(color.BgRed, color.FgHiWhite).Sprint("ERROR"),
	}

	log := slog.New(slogcolor.NewHandler(os.Stderr, &opts))
	return log
}

func SetupViper(cfgFile string) (*viper.Viper, error) {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		airflowHome := os.Getenv("AIRFLOW_HOME")
		if airflowHome == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				return nil, err
			}
			airflowHome = path.Join(home, "airflow")
		}

		// Search config in AIRFLOW_HOME directory with name
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
			return nil, err
		}
	}

	// Make the prefix be AIRFLOW__ -- viper adds an extra `_` automatically
	viper.SetEnvPrefix("AIRFLOW_")
	// Set the key replacer to replace "__" with ".". ie, viper.Get("config.value")
	// looks for the environment variable AIRFLOW__CONFIG__VALUE
	viper.SetEnvKeyReplacer(envKeyReplacer)

	viper.AutomaticEnv() // read in environment variables that match
	return viper.GetViper(), nil
}

// Bind each cobra flag to its associated viper configuration (config file and environment variable)
// This approach cribbed from https://github.com/carolynvs/stingoftheviper/blob/19bd73117f0285436505ca17616cbc394d22e63d/main.go
func BindFlagsToViper(cmd *cobra.Command, viper *viper.Viper) {
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		// Determine the naming convention of the flags when represented in the config file

		var configName string

		if ann, ok := f.Annotations["viper-mapping"]; ok {
			configName = ann[0]
		} else {
			// Skip  the default "help" flag
			if f.Name == "help" {
				return
			}

			// Since viper does case-insensitive comparisons, we don't need to bother fixing the case, and only need to remove the hyphens.
			configName = envKeyReplacer.Replace(f.Name)
		}

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
		} else if f.Value.String() != "" {
			// No changed flag (i.e. default), and no explicit viper set, set the viper value to the flag default
			val := f.Value
			if slice, ok := val.(pflag.SliceValue); ok {
				viper.Set(configName, strings.Join(slice.GetSlice(), " "))
			} else {
				viper.Set(configName, f.Value.String())
			}
		}
	})
}
