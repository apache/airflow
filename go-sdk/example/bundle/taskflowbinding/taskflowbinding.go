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

// Package taskflowbinding contains TaskFlow argument-binding examples.
package taskflowbinding

import (
	"fmt"
	"log/slog"
	"reflect"

	"github.com/apache/airflow/go-sdk/sdk"
)

// Config is an object passed through XCom.
type Config struct {
	Environment string `json:"environment"`
	Region      string `json:"region"`
	Debug       bool   `json:"debug"`
}

// MakeConfig returns a Config XCom.
func MakeConfig(log *slog.Logger) (any, error) {
	cfg := Config{Environment: "production", Region: "eu-west-1", Debug: true}
	log.Info(
		"Pushing config",
		"environment",
		cfg.Environment,
		"region",
		cfg.Region,
		"debug",
		cfg.Debug,
	)
	return cfg, nil
}

// MakeNumbers returns an integer-slice XCom.
func MakeNumbers(log *slog.Logger) (any, error) {
	numbers := []int{1, 1, 2, 3, 5, 8}
	log.Info("Pushing numbers", "numbers", fmt.Sprint(numbers))
	return numbers, nil
}

// MakeRegion returns a region XCom.
func MakeRegion(log *slog.Logger) (any, error) {
	region := "eu-west-1"
	log.Info("Pushing region", "region", region)
	return region, nil
}

// ViaFlatArgs exercises positional binding for literals, defaults, and XComs.
func ViaFlatArgs(
	ctx sdk.TIRunContext,
	log *slog.Logger,
	name string,
	count int,
	ratio float64,
	enabled bool,
	tags []string,
	config Config,
	numbers []int,
	note *string,
) (any, error) {
	if name != "summary" || count != 3 || ratio != 2.5 || !enabled {
		return nil, fmt.Errorf(
			"scalar literals bound incorrectly: name=%q count=%d ratio=%v enabled=%v",
			name, count, ratio, enabled,
		)
	}
	if want := []string{"metrics", "hourly"}; !reflect.DeepEqual(tags, want) {
		return nil, fmt.Errorf("array literal bound incorrectly: tags=%v, want %v", tags, want)
	}
	if want := (Config{Environment: "production", Region: "eu-west-1", Debug: true}); config != want {
		return nil, fmt.Errorf("object XCom bound incorrectly: config=%+v, want %+v", config, want)
	}
	if want := []int{1, 1, 2, 3, 5, 8}; !reflect.DeepEqual(numbers, want) {
		return nil, fmt.Errorf("array XCom bound incorrectly: numbers=%v, want %v", numbers, want)
	}
	if note != nil {
		return nil, fmt.Errorf("defaulted None bound incorrectly: note=%q, want nil", *note)
	}

	sum := 0
	for _, n := range numbers {
		sum += n
	}
	log.InfoContext(ctx, "Bound TaskFlow arguments",
		"name", name,
		"count", count,
		"ratio", ratio,
		"enabled", enabled,
		"tags", fmt.Sprint(tags),
		"environment", config.Environment,
		"sum", sum,
	)
	return map[string]any{
		"name":          name,
		"count":         count,
		"ratio":         ratio,
		"enabled":       enabled,
		"tags":          tags,
		"environment":   config.Environment,
		"debug":         config.Debug,
		"sum":           sum,
		"note_was_null": note == nil,
	}, nil
}

// ViaStructNoTagsInput binds fields by folded Go name.
type ViaStructNoTagsInput struct {
	RegionCode string
	Threshold  float64
}

// ViaStructNoTags exercises binding without `arg:` tags.
func ViaStructNoTags(
	ctx sdk.TIRunContext,
	log *slog.Logger,
	input ViaStructNoTagsInput,
) (any, error) {
	if input.RegionCode != "eu-west-1" || input.Threshold != 0.75 {
		return nil, fmt.Errorf(
			"struct fields bound incorrectly: region_code=%q threshold=%v",
			input.RegionCode,
			input.Threshold,
		)
	}

	log.InfoContext(ctx, "Bound struct (no tags)",
		"region_code", input.RegionCode,
		"threshold", input.Threshold,
	)
	return map[string]any{
		"region_code": input.RegionCode,
		"threshold":   input.Threshold,
	}, nil
}

// ViaStructArgTagInput binds fields with explicit `arg:` tags.
type ViaStructArgTagInput struct {
	Region    string  `arg:"region_code"`
	Threshold float64 `arg:"threshold"`
}

// ViaStructArgTag exercises explicit field-name binding.
func ViaStructArgTag(
	ctx sdk.TIRunContext,
	log *slog.Logger,
	input ViaStructArgTagInput,
) (any, error) {
	if input.Region != "eu-west-1" || input.Threshold != 0.75 {
		return nil, fmt.Errorf(
			"struct fields bound incorrectly: region=%q threshold=%v",
			input.Region,
			input.Threshold,
		)
	}

	log.InfoContext(ctx, "Bound struct (arg: tag)",
		"region", input.Region,
		"threshold", input.Threshold,
	)
	return map[string]any{
		"region":    input.Region,
		"threshold": input.Threshold,
	}, nil
}

// ViaStructUnmatchedArgInput includes a field no argument supplies.
type ViaStructUnmatchedArgInput struct {
	Region  string `arg:"region_code"`
	Missing string `arg:"does_not_exist"`
}

// ViaStructUnmatchedArg exercises unmatched fields and captured defaults.
func ViaStructUnmatchedArg(
	ctx sdk.TIRunContext, log *slog.Logger, input ViaStructUnmatchedArgInput,
) (any, error) {
	if input.Region != "eu-west-1" {
		return nil, fmt.Errorf("struct field bound incorrectly: region=%q", input.Region)
	}
	if input.Missing != "" {
		return nil, fmt.Errorf(
			"expected the unmatched field to stay at its Go zero value, got missing=%q",
			input.Missing,
		)
	}

	log.InfoContext(ctx, "Bound struct (unmatched arg)",
		"region", input.Region,
		"missing_was_empty", input.Missing == "",
	)
	return map[string]any{
		"region":            input.Region,
		"missing_was_empty": input.Missing == "",
	}, nil
}

// FlatMapConfig receives one dict as a whole value.
type FlatMapConfig struct {
	Region string `json:"region"`
	Count  int    `json:"count"`
}

// ViaFlatMap exercises whole-value dict decoding into a struct.
func ViaFlatMap(
	ctx sdk.TIRunContext, log *slog.Logger, config FlatMapConfig,
) (any, error) {
	if config.Region != "eu-west-1" || config.Count != 3 {
		return nil, fmt.Errorf(
			"whole-value map bound incorrectly: region=%q count=%d",
			config.Region,
			config.Count,
		)
	}

	log.InfoContext(ctx, "Bound whole map into struct",
		"region", config.Region,
		"count", config.Count,
	)
	return map[string]any{"region": config.Region, "count": config.Count}, nil
}

// StructMapInput binds a dict to a map field.
type StructMapInput struct {
	Payload map[string]any `arg:"payload"`
}

// ViaStructMap exercises dict binding to a struct field.
func ViaStructMap(
	ctx sdk.TIRunContext, log *slog.Logger, input StructMapInput,
) (any, error) {
	region, _ := input.Payload["region"].(string)
	if region != "eu-west-1" {
		return nil, fmt.Errorf("map field bound incorrectly: payload=%v", input.Payload)
	}

	log.InfoContext(ctx, "Bound map onto struct field", "payload", fmt.Sprint(input.Payload))
	return map[string]any{"payload": input.Payload}, nil
}

// ViaPlainMap exercises dict decoding into a typed map.
func ViaPlainMap(
	ctx sdk.TIRunContext, log *slog.Logger, labels map[string]string,
) (any, error) {
	if labels["team"] != "data" || labels["tier"] != "gold" {
		return nil, fmt.Errorf("plain map bound incorrectly: labels=%v", labels)
	}

	log.InfoContext(ctx, "Bound dict into a plain map", "labels", fmt.Sprint(labels))
	return map[string]any{"team": labels["team"], "tier": labels["tier"]}, nil
}
