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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"github.com/spf13/viper"

	"github.com/apache/airflow/go-sdk/bundle/bundlev1"
	"github.com/apache/airflow/go-sdk/bundle/bundlev1/bundlev1client"
	"github.com/apache/airflow/go-sdk/pkg/logging/shclog"
)

// Discovery handles finding and loading DAG bundles
type Discovery struct {
	logger        *slog.Logger
	hcLogger      hclog.Logger
	bundlesFolder string
	bundles       map[string]map[string]string // bundle_name -> version -> path to binary
}

var BundleNotFound = errors.New("")

// NewDiscovery creates a an object responsible for finding and looking for possible DAG bundle binaries
func NewDiscovery(bundlesFolder string, logger *slog.Logger) *Discovery {
	if logger == nil {
		logger = slog.Default()
	}

	return &Discovery{
		logger:        logger,
		bundlesFolder: bundlesFolder,
		bundles:       make(map[string]map[string]string),
		hcLogger:      shclog.New(logger),
	}
}

func (d *Discovery) versionsForNamedBundle(name string) map[string]string {
	versions, exists := d.bundles[name]
	if exists {
		return versions
	}

	// We couldn't find the specific named bundle

	// First we see if a "default" was configured
	versions, exists = d.bundles[viper.GetString("bundles.default_bundle")]
	if exists {
		return versions
	}

	// Else we see if there is exactly one bundle name registered, and if so we return that
	if len(d.bundles) == 1 {
		exists = true
		for key, versions := range d.bundles {
			// Just pull the first value out
			d.logger.Debug(
				"Using sole bundle as fallback bundle",
				"bundle",
				name,
				"fallback_bundle",
				key,
			)
			return versions
		}
	}
	return nil
}

func (d *Discovery) ClientForBundle(name string, version *string) (*plugin.Client, error) {
	var key string
	if version != nil {
		key = *version
	}

	versions := d.versionsForNamedBundle(name)
	if versions == nil {
		// We couldn't find the specific named bundle
		return nil, fmt.Errorf(
			"%wno dag bundle named %q found (and no fallback suitable)",
			BundleNotFound,
			name,
		)
	}

	cmd, exists := versions[key]
	if !exists {
		// We couldn't find the specific version, but lets see if we have just a single version and use that in
		// its place
		if key == "" && len(versions) == 1 {
			exists = true
			for key, cmd = range versions {
				// Just pull the first value out
				d.logger.Info(
					"Unable to find unversioned bundle as requested, using only version as fallback",
					"bundle",
					name,
					"fallback_version",
					key,
				)
				break
			}
		}
	}

	if !exists {
		if key == "" {
			key = "<unversioned>"
		}
		return nil, fmt.Errorf("%wno version %q found for dag bundle %q", BundleNotFound, key, name)
	}
	return d.makeClient(cmd), nil
}

func (d *Discovery) DiscoverBundles(ctx context.Context) error {
	// Find all files in the bundles directory
	files, err := filepath.Glob(filepath.Join(d.bundlesFolder, "*"))
	if err != nil {
		return fmt.Errorf("failed to read bundles directory: %w", err)
	}

	self, err := os.Executable()
	if err != nil {
		self = ""
	}

	for _, file := range files {
		if ctx.Err() != nil {
			// Check if we are done.
			return ctx.Err()
		}

		// Check if file is executable
		if !isExecutable(file) {
			continue
		}

		abs, err := filepath.Abs(file)
		if err != nil {
			d.logger.Warn("Unable to load resolve file path", "file", file, "err", err)
			continue
		}
		if self != "" && self == abs {
			d.logger.Warn("Not trying to load ourselves as a plugin", "file", file)
			continue
		}

		d.logger.Debug("Found potential bundle", slog.String("path", file))

		// TODO: Use a sync.WaitGroup to parallelize running multiple procs without blowing concurrency up and fork-bombing
		// the host
		bundle, err := d.getBundleVersionInfo(file)
		if err != nil {
			d.logger.Warn("Unable to load BundleMetadata", "file", file, "err", err)
			continue
		}

		versions, exists := d.bundles[bundle.Name]
		if !exists {
			versions = make(map[string]string)
			d.bundles[bundle.Name] = versions
		}

		var key string
		logAs := bundle.Name
		if bundle.Version != nil {
			key = *bundle.Version
			logAs = fmt.Sprintf("%s@%s", bundle.Name, key)
		}
		d.logger.Info(
			"Discovered bundle",
			"key",
			logAs,
			"file",
			file,
		)
		versions[key] = file
	}
	return nil
}

func (d *Discovery) makeClient(binaryPath string) *plugin.Client {
	return plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig:  Handshake,
		Cmd:              exec.Command(binaryPath),
		AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC},
		VersionedPlugins: map[int]plugin.PluginSet{
			1: {"dag-bundle": &bundlev1client.BundleGRPCPlugin{}},
		},
		Logger: d.hcLogger,
	},
	)
}

// getBundleVersionInfo gets version information from a bundle binary
func (d *Discovery) getBundleVersionInfo(binaryPath string) (*bundlev1.BundleInfo, error) {
	client := d.makeClient(binaryPath)
	defer client.Kill()

	rpcClient, err := client.Client()
	if err != nil {
		return nil, err
	}

	raw, err := rpcClient.Dispense("dag-bundle")
	if err != nil {
		return nil, err
	}

	bundle := raw.(bundlev1client.BundleClient)

	metadata, err := bundle.GetMetadata(context.Background())
	if err != nil {
		return nil, err
	}
	return &metadata.Bundle, nil
}

// isExecutable checks if a file is executable
func isExecutable(file string) bool {
	info, err := os.Stat(file)
	if err != nil {
		return false
	}

	// Check if file is regular and has execute permission
	return !info.IsDir() && (info.Mode()&0o111) != 0
}
