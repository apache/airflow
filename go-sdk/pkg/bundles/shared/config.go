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

package shared

import (
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type BundleConfig struct {
	BundlesFolder string `mapstructure:"bundles_folder"`
}

var envKeyReplacer *strings.Replacer = strings.NewReplacer(".", "__", "-", "_")

func SetupViper(cfgFile string) error {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		airflowHome := os.Getenv("AIRFLOW_HOME")
		if airflowHome == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				return err
			}
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

// Bind each cobra flag to its associated viper configuration (config file and environment variable)
// This approach cribbed from https://github.com/carolynvs/stingoftheviper/blob/19bd73117f0285436505ca17616cbc394d22e63d/main.go
func BindFlagsToViper(cmd *cobra.Command) {
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		// Determine the naming convention of the flags when represented in the config file

		var configName string

		if ann, ok := f.Annotations["viper-mapping"]; ok {
			configName = ann[0]
		} else {
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
		}
	})
}
